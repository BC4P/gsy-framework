import configparser

from influxdb import DataFrameClient
import psycopg2
import os 

class Connection:
    def __init__(self, name_config: str):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        path_config = os.path.join(dir_path, "resources", name_config)
        self.config = configparser.ConfigParser()
        if not os.path.isfile(path_config):
            raise Exception(f"File {path_config} does not exist")
        self.config.read(path_config)
    
    #overwrite: need to return the query result
    def query(self, queryString: str):
        pass

    def getDBName(self):
        return self.db
    
    #overwrite: check database connection
    def check(self):
        pass
    
    #overwrite: close db connection
    def close(self):
        pass


class InfluxConnection(Connection):
    def __init__(self, name_influx_config: str):
        super().__init__(name_influx_config)

        self.client = DataFrameClient(
            username=self.config['InfluxDB']['username'],
            password=self.config['InfluxDB']['password'],
            host=self.config['InfluxDB']['host'],
            path=self.config['InfluxDB']['path'],
            port=int(self.config['InfluxDB']['port']),
            ssl=True,
            verify_ssl=True,
            database=self.config['InfluxDB']['database']
        )
        self.db = self.config['InfluxDB']['database']

    def query(self, queryString: str):
        return self.client.query(queryString)
    
    def check(self):
        print(self.client.ping())

    def close(self):
        self.client.close()

    
class PostgreSQLConnection(Connection):
    def __init__(self, name_postgresql_config: str):
        super().__init__(name_postgresql_config)

        params = {}
        params['user']=self.config['PostgreSQL']['user']
        params['password']=self.config['PostgreSQL']['password']
        params['host']=self.config['PostgreSQL']['host']
        params['dbname']=self.config['PostgreSQL']['dbname']
        params['port']=self.config['PostgreSQL']['port']
        self.db = self.config['PostgreSQL']['dbname']

        connection = psycopg2.connect(**params)
        self.client = connection.cursor()

    def close(self):
        self.client.close()
    
    def check(self):
        print('PostgreSQL database version:')
        self.client.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = self.client.fetchone()
        print(db_version)