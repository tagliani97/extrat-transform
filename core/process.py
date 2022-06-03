import os
import sys
import json

from collections import  namedtuple

import arrow

from .log import logger
from .source import get_source
from .services.aws import S3
from .services.connectors import Jdbc
from .services.spark import SparkBlade
from .services.external import TableHive
from .models.connection import get_credentials


class Process:
    def __init__(self, spark, sqlcontext, hivecontext, executors = 10):
        self.spark = spark
        self.sc = spark.sparkContext
        self.sqlcontext = sqlcontext
        self.hivecontext = hivecontext
        self.hadoopconfi = spark._jsc.hadoopConfiguration()

        self._source = None
        
        if(executors is None):
            self.executors = self.sc._jsc.sc().getExecutorMemoryStatus().size()
        else:
            self.executors = executors

    def source(self, source_type, **kwargs):  
        """Adicionar um método de origem de dados
        Parameters
        ----------  
        source_type: str
            Nome do tipo de origem            
        """     
        kwargs["spark"]= self.spark
        kwargs["hivecontext"]= self.hivecontext
        kwargs["sqlcontext"]= self.sqlcontext
        kwargs["executors"]= self.executors
          
        self._source = get_source(source_type, **kwargs)       

    def repair(self, table_name:str, location:str, schema:(dict, str), validation=False, path_reproc=None):
        """Processo de reparação de lacunas aparir de uma origem para o kafka
        Parameters
        ----------  
        table_name: str
            Nome da tabela a ser criada no contexto hive
        location: str
            Endereço de destinos para dados serem reparados
        schema: (dict, str)
            Dicionário com o esquema avro dos dados ou o endereço dos arquivos com o schema         
        validation: bool
            Verificar integridade e repétição dos dados antes deles entrarem(Default: False)
        path_reproc: str
            Endereço de destinos para dados temporários
        """    
        if(self._source is None):
            raise TypeError("No source added")

        time_utc = arrow.utcnow().to('America/Sao_Paulo')
        timestamp = time_utc.format('YYYY-MM-DD')

        table_ext = TableHive(self.hivecontext)
     
        table_ext.create_avro(schema="kafka",
                              table=table_name,
                              location=location,
                              schema_avro=schema,
                              partition_name="day")

        table_ext.repair_partition(schema="kafka", table=table_name)

        dtype = table_ext.get_col_type(schema="kafka", table=table_name)

        dataframe = self._source.run(dtype=dtype, targetValidation=f"kafka.{table_name}" if validation else None)
       
        if(path_reproc is not None):
            SparkBlade.writtenby(dataframe=dataframe, location=path_reproc, num_partition=self.executors)

            table_ext.create_avro(schema="temp",
                                  table=table_name,
                                  location=path_reproc,
                                  schema_avro=schema)
            
            table_ext.insert(source_schema="temp",
                             source_table=table_name,
                             target_schema="kafka", 
                             target_table=table_name, 
                             partition_name="day", 
                             partition_val=timestamp)
        else:
            table_ext.insert_dataframe(dataframe=dataframe, 
                                       target_schema="kafka", 
                                       target_table=table_name, 
                                       partition_name="day", 
                                       partition_val=timestamp)
                                       
        logger.info("Finish blade repair")

    def history(self, path_reproc:str):
        """Processo de Carga histórica dos dados
        Parameters
        ----------  
        path_reproc: str
            Endereço de destinos para dados históricos
        """   
        if(self._source is None):
            raise TypeError("No source added")

        dataframe = self._source.run()
        SparkBlade.writtenby(dataframe=dataframe, location=path_reproc, num_partition=self.executors)
