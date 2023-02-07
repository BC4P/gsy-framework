from gsy_framework.influx_connection.connection import InfluxConnection
from gsy_framework.constants_limits import GlobalConfig
from pendulum import duration
class InfluxQuery:
    def __init__(self, influxConnection: InfluxConnection):
        self.connection = influxConnection
        self.qstring = self.query_string()
    
    def exec(self):
        return self.connection.query(self.qstring)

    def query_string(self,
                duration = duration(days=1),
                start = GlobalConfig.start_date,
                interval = GlobalConfig.slot_length.in_minutes(),
                ):
        return ''

class RawQuery(InfluxQuery):
    def __init__(self, influxConnection: InfluxConnection, querystring: str, procFunc):
        super().__init__(influxConnection)
        self.qstring = querystring
        self.procFunc = procFunc

    def exec(self):
        qresults = super().exec()
        return self.procFunc(qresults)


class DataQuery(InfluxQuery):
    def __init__(self, influxConnection: InfluxConnection, multiplier=1):
        self.multiplier = multiplier
        super().__init__(influxConnection)

    def exec(self):
        qresults = super().exec()
        # Get DataFrame from result
        if(len(list(qresults.values())) != 1):
            print("Load Profile for Query:\n" + self.qstring + "\nnot valid. Using Zero Curve.")
            return os.path.join(d3a_path, "resources", "Zero_Curve.csv")
            

        df = list(qresults.values())[0]

        df = df.reset_index(level=0)

        # remove day from time data
        df["index"] = df["index"].map(lambda x: x.strftime("%H:%M"))

        # remove last row
        df = df.drop(df.tail(1).index)

        # set index to allow converting to dictionary
        df = df.set_index("index")
        df["mean"]  = df["mean"]  * self.multiplier

        ret = df.to_dict().get("mean")
        return ret

class DataQueryMQTT(DataQuery):
    def __init__(self, influxConnection: InfluxConnection,
                        power_column: str,
                        device: str,
                        tablename: str,
                        multiplier=1):
        self.power_column = power_column
        self.device = device
        self.tablename = tablename
        super().__init__(influxConnection, multiplier)
    
    def query_string(self,
                duration = duration(days=1),
                start = GlobalConfig.start_date,
                interval = GlobalConfig.slot_length.in_minutes(),
                ):
        end = start + duration
        return f'SELECT mean("{self.power_column}") FROM "{self.tablename}" WHERE "device" =~ /^{self.device}$/ AND time >= \'{start.to_datetime_string()}\' AND time <= \'{end.to_datetime_string()}\' GROUP BY time({self.interval}m) fill(0)'
