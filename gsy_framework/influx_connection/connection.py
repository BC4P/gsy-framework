import configparser

from influxdb import DataFrameClient
import os 

class InfluxConnection:
    def __init__(self, name_influx_config: str):

        dir_path = os.path.dirname(os.path.realpath(__file__))
        path_influx_config = os.path.join(dir_path, "resources", name_influx_config)
        config = configparser.ConfigParser()
        if not os.path.isfile(path_influx_config):
            raise Exception(f"File {path_influx_config} does not exist")
        config.read(path_influx_config)

        self.client = DataFrameClient(
            username=config['InfluxDB']['username'],
            password=config['InfluxDB']['password'],
            host=config['InfluxDB']['host'],
            path=config['InfluxDB']['path'],
            port=int(config['InfluxDB']['port']),
            ssl=True,
            verify_ssl=True,
            database=config['InfluxDB']['database']
        )
        self.db = config['InfluxDB']['database']

    def query(self, queryString: str):
        return self.client.query(queryString)

    def getDBName(self):
        return self.db