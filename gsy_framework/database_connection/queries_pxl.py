from gsy_framework.database_connection.queries_influx import QuerySingle
from gsy_framework.database_connection.connection import InfluxConnection
from gsy_framework.constants_limits import GlobalConfig
from pendulum import duration

class QueryPXL(QuerySingle):
    def __init__(self, influxConnection: InfluxConnection,
                        power_column: str,
                        tablename: str,
                        duration = duration(days=1),
                        start = GlobalConfig.start_date,
                        interval = GlobalConfig.slot_length.in_minutes(),
                        multiplier = 1.0
                        ):
        self.power_column = power_column
        self.tablename = tablename
        super().__init__(influxConnection, duration, start, interval, multiplier)

    def query_string(self):
        self.qstring = f'SELECT mean("{self.power_column}") FROM "{self.tablename}" WHERE time >= \'{self.start.to_datetime_string()}\' AND time <= \'{self.end.to_datetime_string()}\' GROUP BY time({self.interval}m) fill(0)'