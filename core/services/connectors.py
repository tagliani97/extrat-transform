class Jdbc:    
    def __init__(self, sqlcontext:object, driver:str, url:str, user:str, pwd:str):
        self.sqlcontext = sqlcontext
        self.driver = driver
        self.url = url
        self._user = user
        self._password = pwd

    def write(self, *args, **kwargs):
        raise NotImplementedError()
            
    def read(self, query, partition=None, num_p=None, lower_p=None, upper_p=None, fetch=20000):       
        process = self.sqlcontext.read.format("jdbc").option("url", self.url) \
                                                     .option("user", self._user) \
                                                     .option("password",self._password) \
                                                     .option("driver", self.driver) \
                                                     .option("dbtable", f"({query.strip()})") \
                                                     .option("fetchSize", fetch)
        if(partition is not None):
            process = process.option("partitionColumn", partition) \
                             .option("lowerBound", lower_p) \
                             .option("upperBound", upper_p) \
                             .option("numPartitions", num_p) 
                 
        return process.load()