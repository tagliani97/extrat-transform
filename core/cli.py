

import click


from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext,HiveContext,SparkSession
from core.app import Blade

from .log import logger

configs = SparkConf().set("hive.exec.dynamic.partition.mode", "nonstrict")\
                         .set("hive.mapred.supports.subdirectories", "true")\
                         .set("hive.merge.mapredfiles","False")\
                         .set("hive.merge.mapfiles","False")\
                         .set("mapred.min.split.size","143360")

S3_FORMAT =  "s3a"                  

def get_blade():   
    spark = SparkSession.builder.appName("blade").config(conf=configs).enableHiveSupport().getOrCreate()
    return Blade(spark)

@click.group()
def cli():
    pass

@cli.group("repair")
def repair():
    pass

@cli.group("history")
def history():
   pass

#|---------------------------------|
#|            DATABASE             |
#|---------------------------------|

@repair.command("database")
@click.option('-b', '--base',        'base',        required=True, help='base database origin')
@click.option('-s', '--schema',      'schema',      required=True, help='schema database origin')
@click.option('-t', '--table',       'table',       required=True, help='table database origin')
@click.option('-p', '--primary_key', 'primary_key', required=True, help='primary_key database origin')
@click.option('-i', '--init_range',  'init_range',  type=int, help='init range primary_key database origin')
@click.option('-e', '--end_range',   'end_range',   type=int, help='end range primary_key database origin')
@click.option('-u', '--using_temp',  'using_temp',  required=True, is_flag=True, help='Save file in table reproc and kafka')
@click.option('-v', '--validation',  'validation',  required=True, is_flag=True, help='validate data files in s3 kafka')
@click.option('-k', '--topic_kafka', 'topic_kafka', required=True, help='kafka topic name')
@click.option('-E', '--env',         'env',         required=True, help='enviroment name')
def database_repair(base, schema, table, primary_key, init_range, end_range, using_temp, validation, topic_kafka, env):    
    blade = get_blade()
    if(validation):
        logger.warning(f"validation process is ENABLED")
    blade.repair(source_type="database", 
                 source_args=dict(base=base,  
                                  schema=schema,
                                  table=table,
                                  primary_key=primary_key,
                                  init_range=init_range,
                                  end_range=end_range),
                 using_tmp= using_temp,                
                 validation=validation,
                 target_type="s3",
                 topic_kafka=topic_kafka,
                 env=env)

@history.command("database")
@click.option('-b', '--base',       'base',        required=True, help='base database origin')
@click.option('-s', '--schema',     'schema',      required=True, help='schema database origin')
@click.option('-t', '--table',      'table',       required=True, help='table database origin')
@click.option('-p', '--primary_key','primary_key', required=True, help='primary_key database origin')
@click.option('-E', '--env', 'env', required=True)
def database_history(base, schema, table, primary_key, env):    
    blade = get_blade()
    blade.history(source_type="database",  
                  source_args=dict(base=base, 
                                   schema=schema,
                                   table=table,
                                   primary_key=primary_key),
                  target_type="s3",
                  local_reproc=f"{schema}/{table}",
                  env=env)

#|---------------------------------|
#|               CSV               |
#|---------------------------------|

@repair.command("csv")
@click.option('-t', '--topic_kafka', 'topic_kafka', required=True, help='kafka topic name')
@click.option('-p', '--path',         'path',       required=True)
@click.option('-h', '--header',       'header',     required=True, is_flag=True, default=True)
@click.option('-E', '--env',         'env',         required=True, help='enviroment name')
@click.option('-d', '--delimiter',    'delimiter')
@click.option('-n', '--null_value',   'null_value')
@click.option('-q', '--quote',        'quote')
@click.option('-c', '--charset',      'charset')
def csv_repair(path, header, delimiter, null_value, quote, charset, topic_kafka, env):    
    blade = get_blade()
    path =  path.replace("s3",S3_FORMAT)
    blade.repair(source_type="csv",
                 source_args=dict(path=path,
                                 header=header, 
                                 delimiter=delimiter,
                                 null_value=null_value,
                                 quote=quote,
                                 charset=charset),
                 using_tmp= False,                
                 validation= False,   
                 target_type="s3",
                 topic_kafka=topic_kafka,
                 env=env)

@history.command("csv")
@click.option('-t', '--topic_kafka', 'topic_kafka', required=True, help='kafka topic name')
@click.option('-p', '--path',         'path',       required=True, help='CSV file path in S3')
@click.option('-h', '--header',       'header',     required=True, is_flag=True, default=True, help='set the first line as header')
@click.option('-E', '--env',          'env',        required=True, help='enviroment name')
@click.option('-d', '--delimiter',    'delimiter',  help="columuns delimiter, by default columns are delimited using ','")
@click.option('-n', '--null_value',   'null_value', help='specifies a string that indicates a null value')
@click.option('-q', '--quote',        'quote',      help="by default the quote character is \", but can be set to any character.")
@click.option('-c', '--charset',      'charset',    help="defaults to 'UTF-8' but can be set to other valid charset names")
def csv_history(path, header, delimiter, null_value, quote, charset, topic_kafka, env):    
    blade = get_blade()
    path =  path.replace("s3",S3_FORMAT)
    blade.history(source_type="csv",  
                  source_args=dict(path=path,
                                   header=header, 
                                   delimiter=delimiter,
                                   null_value=null_value,
                                   quote=quote,
                                   charset=charset),
                  local_reproc=f"csv/{topic_kafka}",
                  target_type="s3",
                  env=env)

if __name__ == "__main__":
    cli()