from gsy_framework.influx_connection.queries import DataQuery
from gsy_framework.influx_connection.connection import InfluxConnection
from gsy_framework.constants_limits import GlobalConfig
<<<<<<< HEAD
=======
from pendulum import duration
>>>>>>> 7bfe6fcb267c2b1b0e124916fe9bd42ad7a9ebf6

class DataQueryPXL(DataQuery):
    def __init__(self, influxConnection: InfluxConnection,
                        power_column: str,
<<<<<<< HEAD
                        tablename: str,
                        duration = GlobalConfig.sim_duration,
                        start = GlobalConfig.start_date,
                        interval = GlobalConfig.slot_length.in_minutes(),
                        multiplier = 1.0):
        super().__init__(influxConnection, multiplier)

        end = start + duration
        qstring = f'SELECT mean("{power_column}") FROM "{tablename}" WHERE time >= \'{start.to_datetime_string()}\' AND time <= \'{end.to_datetime_string()}\' GROUP BY time({interval}m) fill(0)'
        self.set(qstring)


class DataQueryPXlMakerspace(DataQuery):
    def __init__(self, influxConnection: InfluxConnection,
                        power_column: str,
                        device: str,
                        tablename: str,
                        duration = GlobalConfig.sim_duration,
                        start = GlobalConfig.start_date,
                        interval = GlobalConfig.slot_length.in_minutes()):
        super().__init__(influxConnection)

        end = start + duration
        qstring = f'SELECT mean("{power_column}") FROM "{tablename}" WHERE "device" =~ /^{device}$/ AND time >= \'{start.to_datetime_string()}\' AND time <= \'{end.to_datetime_string()}\' GROUP BY time({interval}m) fill(0)'

        self.set(qstring)
=======
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
>>>>>>> 7bfe6fcb267c2b1b0e124916fe9bd42ad7a9ebf6
