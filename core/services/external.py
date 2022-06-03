import json

from pathlib import Path
from os.path import dirname

from pyspark.sql.functions import lit

from ..log import logger

class TableHive:
    def __init__(self, sqlcontext):
        self.sqlcontext = sqlcontext
        self.conf = self.sqlcontext.sparkSession.conf
    
    def set_context(self, sqlcontext):
        self.sqlcontext = sqlcontext

    def get_col_type(self, schema, table):
        return self.sqlcontext.table(f"{schema}.{table}").dtypes
    
    def create_avro(self, schema, table, location, schema_avro, partition_name=None, partition_type="string"):
        # Cria tabela external kafka, para realizar o controle de partições antes do insert.
        logger.info(f"Criando external table {schema}.{table}")

        _partition_by = f"PARTITIONED BY ({partition_name} {partition_type})" if partition_name else ""

        # #Athena psdp_RAW TABLE
        self.sqlcontext.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        self.sqlcontext.sql(f"DROP TABLE IF EXISTS {schema}.{table}")

        if(isinstance(schema_avro, dict)):            
            avro_prod = f"'avro.schema.literal'='{json.dumps(json.dumps(schema_avro))[1:-1]}'"

        elif(isinstance(schema_avro, str)):
            avro_prod = f"'avro.schema.url'='{schema_avro}'" 

        else:
            raise ValueError("'schema_avro' accept only types: dict, str")

        sql = f"""CREATE EXTERNAL TABLE {schema}.{table}
            {_partition_by}
            ROW FORMAT SERDE 
                'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
            STORED AS INPUTFORMAT 
                'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
            OUTPUTFORMAT 
                'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
            LOCATION
                '{location}'
            TBLPROPERTIES ({avro_prod})"""
        
        logger.debug(sql)
        
        return self.sqlcontext.sql(sql)
    
    def repair_partition(self, schema, table):
        self.sqlcontext.sql(f"MSCK REPAIR TABLE {schema}.{table}")
        return self.sqlcontext.sql(f"SHOW PARTITIONS {schema}.{table}")
    
    def insert(self, source_schema, source_table, target_schema, target_table, partition_name=None, partition_val=None):
        original = self.conf.get('hive.output.file.extension', "")
        try:
            self.conf.set('hive.output.file.extension', ".avro")
            #Função, que realiza o insert dos dados no bucket kafka, utilizando a external table kafka, para isso.
            logger.info(f"Realizando insert da tabela {source_schema}.{source_table} na tabela de destino {target_schema}.{target_table}")

            if(partition_name is not None and partition_val is not None):
                _par = f"PARTITION({partition_name}='{partition_val}')"
            else:
                _par = ""

            self.sqlcontext.sql(f"""INSERT INTO TABLE {target_schema}.{target_table} {_par}
            SELECT * FROM {source_schema}.{source_table}""")
            
            return self.repair_partition(target_schema, target_table).show()
        finally:
            self.conf.set('hive.output.file.extension', original)

    def insert_dataframe(self, dataframe, target_schema, target_table, partition_name=None, partition_val=None):
        original = self.conf.get('hive.output.file.extension', "")
        try:
            self.conf.set('hive.output.file.extension', ".avro")
            #Função, que realiza o insert dos dados no bucket kafka, utilizando a external table kafka, para isso.
            logger.info(f"Realizando insert do dataframe na tabela de destino {target_schema}.{target_table}")
            cols =  self.sqlcontext.table(f"{target_schema}.{target_table}").columns

            _df = dataframe
            if(partition_name is not None and partition_val is not None):
                _df = _df.withColumn(partition_name,lit(partition_val))

            _df.select(*cols).write.format("hive").mode("append").insertInto(f"{target_schema}.{target_table}")

            return self.repair_partition(target_schema, target_table).show()
        finally:
            self.conf.set('hive.output.file.extension', original)
  