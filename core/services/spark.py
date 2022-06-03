import re
import sys

from pyspark.sql.functions import lit

from ..log import logger

class SparkBlade:    

    @staticmethod
    def validations(context, compareTable, targetTable, p_key):
        #Valida se existem dados do range passado no destino.
        logger.info("Verificando dados no Destino")
        
        validation = context.sql(f"""SELECT t.* FROM {compareTable} t
        JOIN {targetTable} k ON t.{p_key} = k.{p_key} """)

        base_count = context.table(compareTable).count()
        validation_count = validation.count()

        dif = lambda x,y: logger.info(f"Duplicados: {(x - y) * -1}") if y > x else logger.info(f"Faltantes: {(x - y)}")
        logger.info("Origem: {0}, Destino: {1}".format(base_count, validation_count))

        dif(base_count,validation_count)

        if validation_count == base_count:
            context.sql(f"""SELECT k.{p_key}, k.day FROM {targetTable} k
                JOIN {compareTable} t ON k.{p_key} = t.{p_key}""").show()
            logger.info("Dados de Origem, presente no Destino")
            sys.exit()

        #Verifica se os dados do range input estão duplicado no destino.
        elif validation_count > base_count:
            context.sql(f"""WITH kfk(SELECT k.{p_key}, COUNT(k.{p_key})
            FROM {targetTable} k GROUP BY k.{p_key} HAVING COUNT(*) > 1)
            SELECT t.{p_key}
            FROM {compareTable} t
            JOIN kfk ON kfk.{p_key} = t.{p_key}
            ORDER BY t.{p_key} ASC""").show()
            logger.info("Dados Duplicados no Destino")
            sys.exit()

        #Gera um dataframe apenas com range que não existe ná origem.
        elif validation_count < base_count:
            differentdata = context.sql(f"""SELECT t.{p_key} FROM {compareTable} t 
            LEFT OUTER JOIN {targetTable} k ON (k.{p_key} = t.{p_key}) 
            WHERE k.{p_key} IS null ORDER BY t.{p_key} ASC""")
            logger.info("Alterando range Origem")
            dfmin = differentdata.select(f"{p_key}").rdd.min()[0]
            dfmax = differentdata.select(f"{p_key}").rdd.max()[0]
            logger.info(f"Novo range {dfmin} -- {dfmax}")
            
        return dfmin, dfmax

    @staticmethod
    def writtenby(dataframe, location, num_partition, format="avro", mode="overwrite"):
        #Escreve os dados da origem no S3, reproc.
        logger.info("Gerando Arquivo avro")
        return dataframe.write.format(format).mode(mode).save(location)

    @staticmethod
    def rename_outfile(sc, hadoopconf, bucket, prefixo, timestamp, schema, table):
        logger.info("Renomeando Arquivos avro")

        Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        hdfs_dir = f"s3a://{bucket}/{prefixo}/day={timestamp}/"

        fs = Path(hdfs_dir).getFileSystem(hadoopconf)
        regex = r"(.+?-(.+:?)-)"
        list_status = fs.listStatus(Path(hdfs_dir))
        file_name = [file.getPath().getName() for file in list_status if file.getPath().getName().startswith('part-')]
        for old_filename in file_name:
            new_filename = f"BLADE_{schema}.{table}+{re.search(regex, old_filename).groups()[1]}.avro"
            fs.rename(Path(hdfs_dir+''+old_filename),Path(hdfs_dir+''+new_filename))
    
    @classmethod
    def gen_dtype(cls, dtype):    
        new_dtype = []
        for key, types in dtype:
            new_type = types
            if "decimal" in types:
                if ",0" in types:
                    new_type = "long"
                else:
                    new_type = "double"
            new_dtype.append((key, new_type))
        return new_dtype


    @classmethod
    def re_mapping(cls, dataframe, dtype="auto"):
          
        if(isinstance(dtype, str) and dtype.lower() == "auto"):
            logger.info("generating dtype...")
            _dtype = cls.gen_dtype(dataframe.dtypes)

        elif(isinstance(dtype, list)):        
            _dtype = dtype

        else:
            raise ValueError("argumento 'dtype' somente aceita 'auto' ou um lista de tipo do dataframe.")  
        
        logger.info(f"old dtype {dataframe.dtypes}")
        logger.info(f"new dtype {_dtype}")

        new_df = dataframe

        dnewtype = dict((k.lower(),{"col":k,"type":t}) for k, t in _dtype)
        miss_cols = list(dnewtype.keys())        

        for old_key, old_type in dataframe.dtypes:
            try:
                new_type = dnewtype[old_key.lower()]["type"]       
            except:
                logger.warning(f"col '{old_key}' not found in new type")
                continue
            
            if(old_type != new_type):
                new_df = new_df.withColumn(old_key, new_df[old_key].cast(new_type))
            miss_cols.remove(old_key.lower())

        if("dat_kafka" in miss_cols):    
            new_df = new_df.withColumn(dnewtype["dat_kafka"]["col"], lit(None).cast(dnewtype["dat_kafka"]["type"]))
            miss_cols.remove("dat_kafka")
        
        for col in miss_cols:
            logger.warning(f"col '{col}' not found in dataframe")

        return new_df

    @staticmethod
    def rmduplicate(hivecontext, table_db, p_key):
        from  pyspark.sql.functions import input_file_name
        kafka_table = hivecontext.sql(f"""SELECT * FROM kafka.{table_db}""")
        kafka_filename = kafka_table
        kafka_filename = kafka_filename.withColumn("filename", input_file_name())
        kafka_filename.createOrReplaceTempView("kafka_filename")
        try:
            getfile = hivecontext.sql(f""" 
                WITH kfk (    
                    SELECT tmp.{p_key}, tmp.day, tmp.filename
                    FROM (SELECT day, {p_key}, filename, Row_number() over(PARTITION BY {p_key} ORDER BY day) AS rno
                    FROM kafka_filename) as tmp
                    WHERE tmp.rno > 1
                )
                SELECT t.{p_key}, kfk.day, kfk.filename
                FROM temp t
                JOIN kfk ON kfk.{p_key} = t.{p_key}
                ORDER by kfk.{p_key} ASC""")
            s3_client = session.client("s3")
            getfile.select("filename")
            kfk_array = [row.filename for row in getfile.collect()]
            for rmfile in kfk_array:
                bucket, key = re.match(r's3a:\/\/(.+?)\/(.+)', rmfile).groups()
                logger.info(f"Removendo dados Duplicados no Destino..{key}")
                s3_client.delete_object(Bucket=bucket, Key=key)
        except:
            raise ValueError

    