from gsy_framework.influx_connection.queries import QueryAggregated
from gsy_framework.influx_connection.connection import InfluxConnection
from gsy_framework.constants_limits import GlobalConfig
from pendulum import duration

class DataQueryEupen(QueryAggregated):
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
        super().__init__(influxConnection, duration, start, interval)
        
    def query_string(self):
        self.qstring = f'SELECT mean("{self.power_column}") FROM "{self.tablename}" WHERE ("Location" = \'{self.location}\' AND "Key" = \'{self.key}\') AND time >= \'{self.start.to_datetime_string()}\' AND time <= \'{self.end.to_datetime_string()}\' GROUP BY time({self.interval}m), "Meter" fill(0)'