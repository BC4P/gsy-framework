from gsy_framework.influx_connection.queries import InfluxQuery
from gsy_framework.influx_connection.connection import InfluxConnection
from gsy_framework.constants_limits import GlobalConfig
import pandas as pd

class DataQueryEupen(InfluxQuery):
    def __init__(self, influxConnection: InfluxConnection,
                        power_column: str,
                        location: str,
                        key: str,
                        tablename: str,
                        duration = GlobalConfig.sim_duration,
                        start = GlobalConfig.start_date,
                        interval = GlobalConfig.slot_length.in_minutes()):
        self.power_column = power_column
        self.tablename = tablename
        self.key = key
        self.location = location
        super().__init__(influxConnection)
        
    def query_string(self,
                duration = duration(days=1),
                start = GlobalConfig.start_date,
                interval = GlobalConfig.slot_length.in_minutes(),
                ):
        end = start + duration
        return f'SELECT mean("{power_column}") FROM "{tablename}" WHERE ("Location" = \'{location}\' AND "Key" = \'{key}\') AND time >= \'{start.to_datetime_string()}\' AND time <= \'{end.to_datetime_string()}\' GROUP BY time({interval}m), "Meter" fill(0)'

    def _process(self):
        # sum smartmeters
        df = pd.concat(self.qresults.values(), axis=1)
        df = df.sum(axis=1).to_frame("W")

        df.reset_index(level=0, inplace=True)

        # remove day from time data
        df["index"] = df["index"].map(lambda x: x.strftime("%H:%M"))

        # remove last row
        df.drop(df.tail(1).index, inplace=True)
        

        # convert to dictionary
        df.set_index("index", inplace=True)
        df_dict = df.to_dict().get("W")

        return df_dict