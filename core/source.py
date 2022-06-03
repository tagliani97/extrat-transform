from abc import abstractmethod, ABC


from .log import logger
from .services.connectors import Jdbc
from .services.spark import SparkBlade
from .models.connection import get_credentials


class SourceBase(ABC):
    """Classe base para obter os dados a partir de uma base de dados"""
    def __init__(self, spark, hivecontext, sqlcontext, executors):
        """Função inicializadora 
        Parameters
        ----------  
        spark: sparkClass
            Classe principal do spark       
        hivecontext: HiveContext
            Classe de contexto do Hive SQL
        sqlcontext: SqlContext
            Classe de contexto do spark SQL
        executors: int
            Número de executores do spark
        """
        self.spark = spark
        self.sc = spark.sparkContext
        self.sqlcontext = sqlcontext
        self.hivecontext = hivecontext
        self.hadoopconfi = spark._jsc.hadoopConfiguration()
        self.executors = executors

    @abstractmethod
    def run(self, executors, targetValidation=None, dtype="auto"):
        """Função abstrata para o processo de obtentação de dados
        Parameters
        ----------  
        targetValidation: str
            Nome da tabela no contexto hive que será usado como validador. (default: none)
        dtype:str | list
            Lista com as tipagens dos campos para conversão. Caso auto, os campo serão convertidos para o padrão(default: auto)
        
        Return
        ------
            SparkDataFrame
        """
        pass

class _SourceDatabase(SourceBase):
    """Classe para obter os dados a partir de uma base de dados"""
    def __init__(self, base, schema, table, primary_key, num_partition=None, init_range=None, end_range=None, *args, **kwargs):
        """Função inicializadora
        Parameters
        ----------         
        executors: int
            Número de executores do spark
        base: str
            Nome da base de dados (ex: otis)
        schema: str
            Nome do schema
        table: str
            Nome da tabela
        primary_key: str
            Nome do campo da chave primária
        num_partition: int
            Número de partições que os dados conterão. Caso nulo, será utilizado o número de executors(Default: None) 
        init_range: int
            Valor inicial da chave primária. Caso nulo, será obtido automaticamente(Default: None) 
        end_range: int
            Valor final da chave primária. Caso nulo, será obtido automaticamente(Default: None) 
        """
        super().__init__(*args, **kwargs)

        self.base=base
        self.schema=schema
        self.table=table
        self.primary_key=primary_key
        self.num_partition = self.executors if num_partition is None else num_partition
        self.init_range = init_range
        self.end_range=end_range
        credentials = get_credentials(base)
        self.jdbc_control = Jdbc(self.sqlcontext, driver="oracle.jdbc.OracleDriver", 
                                                  url=credentials["jdbc"],
                                                  user=credentials["user"],
                                                  pwd=credentials["pass"])       

    def _get_range(self):
        getpk = f"""SELECT MIN({self.primary_key}) initrange, MAX({self.primary_key}) endrange
                FROM {self.schema}.{self.table}"""

        query = f"({getpk.strip()})"
        jdbcDF = self.jdbc_control.read(query=query)

        initrange = int(jdbcDF.select("initrange").head()[0])
        endrange = int(jdbcDF.select("endrange").head()[0])
        return initrange, endrange

    def run(self, targetValidation=None, dtype="auto"):
        """Executar o processo de obtentação de dados
        Parameters
        ----------  
        targetValidation: str
            Nome da tabela no contexto hive que será usado como validador. (default: none)
        dtype:str | list
            Lista com as tipagens dos campos para conversão. Caso auto, os campo serão convertidos para o padrão(default: auto)
        
        Return
        ------
            SparkDataFrame
        """
        if(self.init_range is None or self.end_range is None ):
            initrange, endrange = self._get_range()
        else:
            initrange = self.init_range
            endrange  = self.end_range

        #Gera a query utilizada na origem, com isso seu retorno será um dataframe com dados para insert no bucket kafka.
        sql = f"""select '{self.schema}.{self.table}' as "table", 'null' as "op_type", 'null' as "op_ts"
            , to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as "current_ts"
            , 'null' as "pos"
            , t.* 
            from {self.schema}.{self.table} t
            where 1=1
            and t.{self.primary_key} >= {initrange}
            and t.{self.primary_key} <= {endrange}"""

        query = f"({sql.strip()})"
        logger.info("Consulta na Origem")
        logger.info(query)

        df = self.jdbc_control.read(query=query,
                                    partition=self.primary_key, 
                                    num_p=self.num_partition, 
                                    lower_p=initrange, 
                                    upper_p=endrange, 
                                    fetch=50000)

        dfwrite = SparkBlade.re_mapping(df, dtype=dtype)

        if targetValidation is not None:
            dfwrite.createOrReplaceTempView("tempCompare")

            newInit, newEnd = SparkBlade.validations(context=self.hivecontext, 
                                                     compareTable="tempCompare", 
                                                     targetTable=targetValidation, 
                                                     p_key=self.primary_key)

            query = query.replace(str(initrange),str(newInit)).replace(str(endrange),str(newEnd))
            logger.info(query)

            df = self.jdbc_control.read(query=sql,
                                        partition=self.primary_key, 
                                        num_p=self.num_partition, 
                                        lower_p=newInit, 
                                        upper_p=newEnd, 
                                        fetch=30000)

            dfwrite = SparkBlade.re_mapping(df, dtype=dtype)
      
        return dfwrite   


class _SourceCSV(SourceBase):
    """Classe para obter os dados a partir de um arquivo csv"""
    def __init__(self, path, header=True, delimiter=None, null_value=None, quote=None, 
                charset=None, *args, **kwargs):
        
        """Função inicializadora
        Parameters
        ----------         
        path: str
            Caminho do bucket aonde se encontra o arquivo CSV a ser carregado
        header: str
            Quando setado 'true' a primeira linha do arquivo será usada como cabeçalho e não será incluída nos dados (default: 'true')
        delimiter: str
            Caractere delimitador de separação de colunas no arquivo CSV (default: ',')
        null_value: str
            Valor reconhecido como nulo na leitura do CSV (default: '')
        quote:str
            Caractere que define as aspas, entre as apas qualquer delimitador é ignorado (default: '"')
        charset:str
            Define o charset para leitura do arquivo carregado (default: 'UTF-8')
        """
        super().__init__(*args, **kwargs)

        self.path   =path 
        self.header =header
        self.options=dict(delimiter=delimiter,
                          null_value=null_value,
                          quote=quote,
                          charset=charset)

        self.options={k: v for k, v in self.options.items() if v is not None}
        
    def run(self, dtype="auto", *args, **kwargs):
        """Executar o processo de obtentação de dados
        Parameters
        ----------  
        dtype:str | list
            Lista com as tipagens dos campos para conversão. Caso auto, os campo serão convertidos para o padrão(default: auto)
        
        Return
        ------
            SparkDataFrame
        """
        df = self.sqlcontext.read.format("csv").options(header=self.header, **self.options)
        df = df.load(self.path)
        dfwrite = SparkBlade.re_mapping(df, dtype=dtype)

        return dfwrite


def get_source(source_type, *args, **kwargs) -> SourceBase:
    """Obter a classe de source
    Parameters
    ---------- 
    source_type: str
        Tipo da origem
    Return
    ------
        SourceBase
    """
    try:    
        _source = _SourceType.get(source_type)
    except KeyError:
        raise TypeError(f"Type source '{source_type}' does not exist. ({', '.join(_SourceType.keys())})")

    return _source(*args, **kwargs)


_SourceType = dict(database=_SourceDatabase, csv=_SourceCSV)
