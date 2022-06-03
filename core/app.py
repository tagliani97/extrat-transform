import os
import sys
import json

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext, SparkSession

from .log import logger
from .config import settings 
from .process import Process
from .services.aws import S3


class Blade:
    """Classe com os processos automatizados do blade"""
    def __init__(self, spark, executors = 40):
        """Função inicializadora 
        Parameters
        ----------  
        spark: sparkClass
            Classe principal do spark
        executors: int
            Número de executores do spark
        """
        self.spark = spark
        self.sc = spark.sparkContext
        self.sqlcontext = SQLContext(spark)
        self.hivecontext = HiveContext(spark)
        self.hadoopconfi = spark._jsc.hadoopConfiguration()

        
        if(executors is None):
            self.executors = self.sc._jsc.sc().getExecutorMemoryStatus().size()
        else:
            self.executors = executors
     
    def repair(self,  source_type:str, target_type:str, table_name="temp_repair", source_args={}, using_tmp=False, validation=False, env=None,  **target_args):
        """Executar o processo de reparação de lacunas nos dados
        Parameters
        ----------          
        table_name: str
            Nome da tabela no contexto hive        
        source_type: str
            Tipo da origem dos dados
        target_type: str
            Tipo do destino dos dados (s3, local)
        source_args: dict
            Dicionário com os argumentos utilizandos no tipo da origem especificado
        using_tmp: bool
            Usar uma tabela temporária para salvar os dados antes de enviar para a kafka
        env: str
            Nome do ambiente
        **target_args: dict
            Demais argumentos usados para o tipo de destino
        """
        settings.setenv(env)
        if(target_type=="s3"): 
            _prefix_kafka =  target_args.get("prefix_kafka",  settings.get("prefix_kafka",  None))
            _prefix_reproc = target_args.get("prefix_reproc", settings.get("prefix_reproc", None))
            _bucket_kafka =  target_args.get("bucket_kafka",  settings.get("bucket_kafka",  None))
            _bucket_reproc = target_args.get("bucket_reproc", settings.get("bucket_reproc", None))
            _topic_kafka =   target_args.get("topic_kafka", None)

            if(_prefix_kafka is None  or _bucket_kafka is None or _topic_kafka is None):
                raise ValueError("target type 's3' needs the variables: (prefix_kafka, bucket_kafka, topic_kafka)")

            prefix_kafka = f"{_prefix_kafka}/{_topic_kafka}"  
            path_kafka = f"s3a://{_bucket_kafka}/{prefix_kafka}"
            
            # if(using_tmp):
            #     if(_prefix_reproc is None or _bucket_reproc is None):
            #         raise ValueError("The 'using_tmp' argument needs to be accompanied by: (prefix_reproc, bucket_reproc)")

            #     path_reproc = f"s3a://{_bucket_reproc}/{_prefix_reproc}/{schema.upper()}/{table.upper()}"
            # else:
            #     path_reproc= None
            path_reproc= None
            
            S3.set_hadoop_conf(self.hadoopconfi)
            
            _schema_kafka = S3.get_schema(bucket=_bucket_kafka, key=prefix_kafka)
            _prefix_schema = f"blade/executions/repair/{_topic_kafka}/schema.avsc"
            S3.write_object(bucket=_bucket_kafka, key=_prefix_schema, content=json.dumps(_schema_kafka))
            schema_kafka = f"s3a://{_bucket_kafka}/{_prefix_schema}"


        elif(target_type=="local"):
            path_reproc = target_args.get("local_reproc", settings.get("local_reproc", None))
            path_kafka = target_args.get("local_target", settings.get("local_target", None))
            schema_kafka = target_args.get("schema_kafka", None)

            if( schema_kafka is None or path_kafka is None):
                raise ValueError("target type 'local' needs the variables: (local_target, schema_kafka)")

            if(using_tmp and (path_reproc is None)):
                raise ValueError("The 'using_tmp' argument needs to be accompanied by: (local_reproc)")
        else:
            raise ValueError("target_type accepts only the value: (s3, local)")

        bladeProcess = Process(spark=self.spark, 
                               sqlcontext=self.sqlcontext, 
                               hivecontext=self.hivecontext, 
                               executors=self.executors)
                               
        bladeProcess.source(source_type, **source_args)
        bladeProcess.repair(table_name=table_name, location=path_kafka, schema=schema_kafka, path_reproc=path_reproc, validation=validation)
        
        if(target_type=="s3"):
            logger.info("Remove trash from bucket")
            S3.remove_trash(bucket=_bucket_kafka, key=prefix_kafka)


    def history(self, source_type:str, source_args:dict, target_type:str, local_reproc:str, env=None, **target_args):
        """Executar o processo de carga histórica
        Parameters
        ----------       
        source_type: str
            Tipo da origem dos dados        
        source_args: dict
            Dicionário com os argumentos utilizandos no tipo da origem especificado
        target_type: str
            Tipo do destino dos dados (s3, local)
        env: str
            Nome do ambiente   
        **target_args: dict
            Demais argumentos usados para o tipo de destino
        """
        settings.setenv(env)
        if(target_type=="s3"):            
            _prefix_history = target_args.get("prefix_history", settings.get("prefix_history", None))            
            _bucket_reproc = target_args.get("bucket_reproc", settings.get("bucket_reproc", None))

            if( _prefix_history is None or _bucket_reproc is None):
                raise ValueError("target type 's3' needs the variables: ( prefix_reproc, bucket_reproc)")

            path_reproc = os.path.join("s3a://", _bucket_reproc, _prefix_history, local_reproc).replace("\\","/")

        elif(target_type=="local"):
            path_reproc = local_reproc           

        else:
            raise ValueError("target_type accepts only the value: (s3, local)")

        bladeProcess = Process(spark=self.spark, 
                               sqlcontext=self.sqlcontext, 
                               hivecontext=self.hivecontext, 
                               executors=self.executors)
        bladeProcess.source(source_type, **source_args)
        bladeProcess.history(path_reproc=path_reproc)
