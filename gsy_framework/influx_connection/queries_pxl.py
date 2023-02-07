from gsy_framework.influx_connection.queries import DataQuery
from gsy_framework.influx_connection.connection import InfluxConnection
from gsy_framework.constants_limits import GlobalConfig
from pendulum import duration

class DataQueryPXL(DataQuery):
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
        return f'SELECT mean("{self.power_column}") FROM "{self.tablename}" WHERE time >= \'{start.to_datetime_string()}\' AND time <= \'{end.to_datetime_string()}\' GROUP BY time({interval}m) fill(0)'
