from gsy_framework.database_connection.queries_base import QueryRaw
from gsy_framework.database_connection.queries_influx import QuerySingle, QueryAggregated
from gsy_framework.database_connection.queries_postgresql import QueryPostgresSQL
from gsy_framework.database_connection.connection import InfluxConnection, PostgreSQLConnection
from gsy_framework.constants_limits import GlobalConfig
from pendulum import duration

class QuerySmartmeterID(QueryRaw):
    def __init__(self, influxConnection: InfluxConnection, keyname: str):
        qstring = f'SHOW TAG VALUES ON "{influxConnection.getDBName()}" WITH KEY IN ("{keyname}")'

        def transform():
            return [point["value"] for point in list(self.qresults.get_points())]
            
        super().__init__(influxConnection, qstring, transform)

    


# class SingleDataPointQuery(Query):
#     def __init__(self, influxConnection: InfluxConnection,
#                         power_column: str,
#                         tablename: str,
#                         smartmeterID: str,
#                         slot_length = GlobalConfig.slot_length):
#         super().__init__(influxConnection)

#         self.qstring = f'SELECT mean("{power_column}") FROM "{tablename}" WHERE "id" = \'{smartmeterID}\' AND time >= now() - {slot_length.in_minutes()}m'
    
#     def exec(self):
#         qresults = super().exec()
#         value_list = list(qresults.values())[0]
#         value_list.reset_index(level=0, inplace=True)
#         return value_list["index"][0], value_list["mean"][0]



class QueryFHAC(QuerySingle):
    def __init__(self, influxConnection: InfluxConnection,
                        power_column: str,
                        tablename: str,
                        smartmeterID: str,
                        multiplier=1.0,
                        duration = duration(days=1),
                        start = GlobalConfig.start_date,
                        interval = GlobalConfig.slot_length.in_minutes()
                        ):
        self.power_column = power_column
        self.smartmeterID = smartmeterID
        self.tablename = tablename
        super().__init__(influxConnection, duration, start, interval, multiplier)

    def query_string(self):
        self.qstring = f'SELECT mean("{self.power_column}") FROM "{self.tablename}" WHERE "id" = \'{self.smartmeterID}\' AND time >= \'{self.start.to_datetime_string()}\' AND time <= \'{self.end.to_datetime_string()}\' GROUP BY time({self.interval}m) fill(0)'

class QueryFHACAggregated(QueryAggregated):
    def __init__(self, influxConnection: InfluxConnection,
                        power_column: str,
                        tablename: str,
                        duration = duration(days=1),
                        start = GlobalConfig.start_date,
                        interval = GlobalConfig.slot_length.in_minutes()
                        ):
        self.power_column = power_column
        self.tablename = tablename
        super().__init__(influxConnection, duration, start, interval)

    def query_string(self):
        self.qstring = f'SELECT mean("{self.power_column}") FROM "{self.tablename}" WHERE time >= \'{self.start.to_datetime_string()}\' AND time <= \'{self.end.to_datetime_string()}\' GROUP BY time({self.interval}m), "id" fill(0)'

class QueryFHACPV(QueryPostgresSQL):
    def __init__(self, postgresConnection: PostgreSQLConnection,
                        plant: str,
                        tablename: str,
                        duration = duration(days=1),
                        start = GlobalConfig.start_date,
                        interval = GlobalConfig.slot_length.in_minutes()
                        ):
        self.plant = plant
        self.tablename = tablename
        super().__init__(postgresConnection, duration, start, interval)

    def query_string(self):
        self.qstring = f'SELECT time_bucket(\'{self.interval}m\',datetime), avg(value) FROM {self.tablename} WHERE datetime BETWEEN \'{self.start.to_datetime_string()}\' AND \'{self.end.to_datetime_string()}\' AND plant = \'{self.plant}\' GROUP BY 1 ORDER BY 1'