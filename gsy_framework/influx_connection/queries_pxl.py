from gsy_framework.influx_connection.queries import DataQuery
from gsy_framework.influx_connection.connection import InfluxConnection
from gsy_framework.constants_limits import GlobalConfig

class DataQueryPXL(DataQuery):
    def __init__(self, influxConnection: InfluxConnection,
                        power_column: str,
                        tablename: str,
                        duration = GlobalConfig.sim_duration,
                        start = GlobalConfig.start_date,
                        interval = GlobalConfig.slot_length.in_minutes()):
        super().__init__(influxConnection)

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