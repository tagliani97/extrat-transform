import click, boto3

@click.group()
def params():
    pass


@params.command()
@click.option('--cluster',      prompt='cluster' ,             help='ID Cluster running')
@click.option('--env',      prompt='env' ,             help='aws account env')
@click.option('--base',         prompt='base' ,                help='base database origin')
@click.option('--schema',       prompt='schema' ,              help='schema database origin')
@click.option('--table',        prompt='table' ,               help='table database origin')
@click.option('--primary_key',  prompt='primary_key' ,         help='primary_key database origin')
@click.option('--init_range',   prompt='init_range', type=int, help='init range primary_key database origin')
@click.option('--end_range',    prompt='end_range',  type=int, help='end range primary_key database origin')
@click.option('--topic_kafka', prompt='topic_kafka',         help='prefix data file kafka')
@click.option('--validation',  required=True, is_flag=True, help='validate data files in s3 kafka')
def repair(cluster,env,base,schema,table,primary_key,init_range,end_range,topic_kafka,validation):
    """Correcao de range"""
    session = boto3.Session()
    client = session.client("emr")
    arg = ['spark-submit','--deploy-mode','client',
        '/home/hadoop/blade.py',
        'repair',
        'database'] + click.get_os_args()[1:]

    not_step_args= ['--cluster', cluster]
    arg = [param for param in arg 
            if param not in not_step_args]

    response = client.add_job_flow_steps(
    JobFlowId=f'{cluster}',
    Steps=[{
            'Name': f'{schema}.{table}',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': arg}
            }
        ]
    )
    print(response)


@params.command()
@click.option('--env',      prompt='env' ,             help='aws account env')
@click.option('--base'        ,prompt='base'        ,help='base database origin')
@click.option('--schema'      ,prompt='schema'      ,help='schema database origin')
@click.option('--table'       ,prompt='table'       ,help='table database origin')
@click.option('--primary_key' ,prompt='primary_key' ,help='primary_key database origin')
@click.option('--cluster'         ,prompt='cluster'     ,help='ID Cluster running')
def history(cluster,env,base,schema,table,primary_key):
    """Carga historica"""
    session = boto3.Session()
    client = session.client("emr")
    arg = ['spark-submit','--deploy-mode','client',
    '/home/hadoop/blade.py','history',
    'database'] + click.get_os_args()[1:]

    not_step_args= ['--cluster',cluster]
    arg = [param for param in arg 
            if param not in not_step_args]

    response = client.add_job_flow_steps(
    JobFlowId=f'{cluster}',
    Steps=[{
            'Name': f'{schema}.{table}',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': arg}
            }
        ]
    )
    print(response)


@params.command()
@click.option('--cluster',      prompt='cluster' ,             help='ID Cluster running')
@click.option('--env',          prompt='env' ,                 help='aws account env')
@click.option('--path',         prompt='path',       required=True, help='S3 path where the CSV file is')
@click.option('--topic_kafka',  prompt='topic_kafka', required=True, help='kafka topic name')
@click.option('--header',       required=True, help='if it is true, the first row will be ignored', is_flag=True, default=True)
@click.option('--delimiter',    help='delimiter that separates columns', default=None)
@click.option('--null_value',   help='any character passed will be recognized as null', default=None)
@click.option('--quote',        help='get a char that defines quote marks, beteween quotes any delimiter will be ignored', default=None)
@click.option('--charset',      help='defines file charset', default=None)
def csv_repair(cluster,env, path, header, delimiter, null_value, quote, charset, topic_kafka):
    """Carregar arquivo csv para o S3"""
    session = boto3.Session()
    client = session.client("emr")
    arg = ['spark-submit','--deploy-mode','client',
            '/home/hadoop/blade.py',
             'repair',
             'csv'] + click.get_os_args()[1:]

    not_step_args= ['--cluster', cluster]
    arg = [param for param in arg 
            if param not in not_step_args]

    response = client.add_job_flow_steps(
    JobFlowId=f'{cluster}',
    Steps=[{
            'Name': f'blade-csv-{topic_kafka}',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': arg}
            }
        ]
    )
    print(response)

@params.command()
@click.option('--cluster',      prompt='cluster' ,             help='ID Cluster running')
@click.option('-t', '--topic_kafka', 'topic_kafka', required=True, help='kafka topic name')
@click.option('-p', '--path',         'path',       required=True, help='CSV file path in S3')
@click.option('-h', '--header',       'header',     required=True, is_flag=True, default=True, help='set the first line as header')
@click.option('-E', '--env',          'env',        required=True, help='enviroment name')
@click.option('-d', '--delimiter',    'delimiter',  help="columuns delimiter, by default columns are delimited using ','")
@click.option('-n', '--null_value',   'null_value', help='specifies a string that indicates a null value')
@click.option('-q', '--quote',        'quote',      help="by default the quote character is \", but can be set to any character.")
@click.option('-c', '--charset',      'charset',    help="defaults to 'UTF-8' but can be set to other valid charset names")
def csv_history(cluster,env, path, header, delimiter, null_value, quote, charset, topic_kafka):
    """Carregar arquivo csv para o S3"""
    session = boto3.Session()
    client = session.client("emr")
    arg = ['spark-submit','--deploy-mode','client',
            '/home/hadoop/blade.py',
             'history',
             'csv'] + click.get_os_args()[1:]

    not_step_args= ['--cluster', cluster]
    arg = [param for param in arg 
            if param not in not_step_args]

    response = client.add_job_flow_steps(
    JobFlowId=f'{cluster}',
    Steps=[{
            'Name': f'blade-csv-{topic_kafka}',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': arg}
            }
        ]
    )
    print(response)

if __name__ == '__main__':
    params()

