from gsy_framework.influx_connection.queries import InfluxQuery, DataQuery
from gsy_framework.influx_connection.connection import InfluxConnection
from gsy_framework.constants_limits import GlobalConfig
import pandas as pd
from pendulum import duration

class SmartmeterIDQuery(InfluxQuery):
    def __init__(self, influxConnection: InfluxConnection, keyname: str):
        super().__init__(influxConnection)
        self.qstring = f'SHOW TAG VALUES ON "{self.connection.getDBName()}" WITH KEY IN ("{keyname}")'

    def exec(self):
        qresults = super().exec()
        points = list(qresults.get_points())
        return [point["value"] for point in points]


class SingleDataPointQuery(InfluxQuery):
    def __init__(self, influxConnection: InfluxConnection,
                        power_column: str,
                        tablename: str,
                        smartmeterID: str,
                        slot_length = GlobalConfig.slot_length):
        super().__init__(influxConnection)

        self.qstring = f'SELECT mean("{power_column}") FROM "{tablename}" WHERE "id" = \'{smartmeterID}\' AND time >= now() - {slot_length.in_minutes()}m'
    
    def exec(self):
        qresults = super().exec()
        value_list = list(qresults.values())[0]
        value_list.reset_index(level=0, inplace=True)
        return value_list["index"][0], value_list["mean"][0]



class DataQueryFHAachen(DataQuery):
    def __init__(self, influxConnection: InfluxConnection,
                        power_column: str,
                        tablename: str,
                        smartmeterID: str,
                        multiplier=1):
        self.power_column = power_column
        self.smartmeterID = smartmeterID
        self.tablename = tablename
        super().__init__(influxConnection,multiplier)

    def query_string(self,
                duration = duration(days=1),
                start = GlobalConfig.start_date,
                interval = GlobalConfig.slot_length.in_minutes(),
                ):
        end = start + duration
        return f'SELECT mean("{self.power_column}") FROM "{self.tablename}" WHERE "id" = \'{self.smartmeterID}\' AND time >= \'{start.to_datetime_string()}\' AND time <= \'{end.to_datetime_string()}\' GROUP BY time({interval}m) fill(0)'

class DataFHAachenAggregated(InfluxQuery):
    def __init__(self, influxConnection: InfluxConnection,
                        power_column: str,
                        tablename: str):
        self.power_column = power_column
        self.tablename = tablename
        super().__init__(influxConnection)

    def query_string(self,
                duration = duration(days=1),
                start = GlobalConfig.start_date,
                interval = GlobalConfig.slot_length.in_minutes(),
                ):
        end = start + duration
        return f'SELECT mean("{self.power_column}") FROM "{self.tablename}" WHERE time >= \'{start.to_datetime_string()}\' AND time <= \'{end.to_datetime_string()}\' GROUP BY time({interval}m), "id" fill(0)'

    def exec(self):
        qresults = super().exec()
        # sum smartmeters
        df = pd.concat(qresults.values(), axis=1)
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