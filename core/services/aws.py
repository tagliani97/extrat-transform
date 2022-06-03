import io
import json

import boto3

from fastavro import reader

from ..log import logger

class S3:
    session = boto3.Session()

    @classmethod
    def set_profile(cls, profile):
        cls.session = boto3.Session(profile_name=profile)

    @classmethod
    def write_object(cls, bucket, key, content):
        bucket = cls.session.resource("s3").Bucket(bucket)
        bucket.Object(key=key).put(Body=content)

    @classmethod
    def list_objects(cls, bucket, key):
        s3_client = cls.session.client('s3')
        list_objects_v2_page = s3_client.get_paginator('list_objects_v2')
        paginate = list_objects_v2_page.paginate(Bucket=bucket, Prefix=key)
        for page in paginate:
            for obj in page["Contents"]:
                yield obj

    @classmethod
    def get_schema(cls, bucket, key):
        #Busca o avro no s3, faz a leitura em binário, para poder gerar schema literal.
        logger.info("Schema literal location: {}/{}".format(bucket,key))
        
        objs = cls.list_objects(bucket, key)
        objs = filter(lambda o: 'day' in o["Key"], objs)
        file_key = sorted(objs, reverse=True, key=lambda x:x["LastModified"])[0]["Key"]
        
        s3_client = cls.session.client('s3')

        file_obj = s3_client.get_object(Bucket=bucket, Key=file_key)
        avro_reader = reader(io.BytesIO(file_obj['Body'].read()))      
        
        return avro_reader.writer_schema

    @classmethod
    def remove_trash(cls, bucket, key):
        s3_client = cls.session.client("s3")
        list_objects_v2_page = s3_client.get_paginator('list_objects_v2')
        paginate = list_objects_v2_page.paginate(Bucket=bucket, Prefix=key)
        for page in paginate:
            for obj in page['Contents']:
                if 'HIVE_DEFAULT_PARTITION' in obj['Key']:
                    file = obj['Key']
                    try:
                        logger.info("removing object: {0}").format(file)
                        s3_client.delete_object(Bucket=bucket, Key=file)
                    except:
                        raise ValueError

    @classmethod
    def set_hadoop_conf(cls, hadoop_conf):        
        hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        hadoop_conf.set("fs.s3a.fast.upload", "true")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.experimental.input.fadvise", "random")
        hadoop_conf.set("fs.s3a.path.style.access", "true")
