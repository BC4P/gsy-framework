<<<<<<< HEAD
from gsy_framework.influx_connection.connection import InfluxConnection
=======
import os

from pendulum import duration

from gsy_framework.influx_connection.connection import InfluxConnection
from gsy_framework.constants_limits import GlobalConfig
from gsy_e.gsy_e_core.util import d3a_path
>>>>>>> 7bfe6fcb267c2b1b0e124916fe9bd42ad7a9ebf6

class InfluxQuery:
    def __init__(self, influxConnection: InfluxConnection):
        self.connection = influxConnection
<<<<<<< HEAD
    
    def set(self, querystring: str):
        self.qstring = querystring

    def exec(self):
        self.qresults = self.connection.query(self.qstring)
        return self._process()

    def _process(self):
        pass
=======
        self.qstring = self.query_string()
    
    def exec(self):
        return self.connection.query(self.qstring)

    def query_string(self,
                duration = duration(days=1),
                start = GlobalConfig.start_date,
                interval = GlobalConfig.slot_length.in_minutes(),
                ):
        return ''
>>>>>>> 7bfe6fcb267c2b1b0e124916fe9bd42ad7a9ebf6

class RawQuery(InfluxQuery):
    def __init__(self, influxConnection: InfluxConnection, querystring: str, procFunc):
        super().__init__(influxConnection)
<<<<<<< HEAD
        self.set(querystring)
        self.procFunc = procFunc

    def _process(self):
        return self.procFunc(self.qresults)


class DataQuery(InfluxQuery):
    def __init__(self, influxConnection: InfluxConnection):
        super().__init__(influxConnection)

    def _process(self):
        # Get DataFrame from result
        if(len(list(self.qresults.values())) != 1):
            return False;

        df = list(self.qresults.values())[0]

        df.reset_index(level=0, inplace=True)
=======
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
>>>>>>> 7bfe6fcb267c2b1b0e124916fe9bd42ad7a9ebf6

        # remove day from time data
        df["index"] = df["index"].map(lambda x: x.strftime("%H:%M"))

        # remove last row
<<<<<<< HEAD
        df.drop(df.tail(1).index, inplace=True)

        # convert to dictionary
        df.set_index("index", inplace=True)
        ret = df.to_dict().get("mean")
        return ret
=======
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
        return f'SELECT mean("{self.power_column}") FROM "{self.tablename}" WHERE "device" =~ /^{self.device}$/ AND time >= \'{start.to_datetime_string()}\' AND time <= \'{end.to_datetime_string()}\' GROUP BY time({interval}m) fill(0)'
>>>>>>> 7bfe6fcb267c2b1b0e124916fe9bd42ad7a9ebf6
