from gsy_framework.influx_connection.connection import InfluxConnection
from gsy_framework.constants_limits import GlobalConfig

class InfluxQuery:
    def __init__(self, influxConnection: InfluxConnection):
        self.connection = influxConnection
    
    def set(self, querystring: str):
        self.qstring = querystring

    def exec(self):
        self.qresults = self.connection.query(self.qstring)
        return self._process()

    def getQueryString(self):
        return self.qstring

    def _process(self):
        pass

class RawQuery(InfluxQuery):
    def __init__(self, influxConnection: InfluxConnection, querystring: str, procFunc):
        super().__init__(influxConnection)
        self.set(querystring)
        self.procFunc = procFunc

    def _process(self):
        return self.procFunc(self.qresults)


class DataQuery(InfluxQuery):
    def __init__(self, influxConnection: InfluxConnection, invert=False):
        self.invert = invert
        super().__init__(influxConnection)

    def _process(self):
        # Get DataFrame from result
        if(len(list(self.qresults.values())) != 1):
            return False;

        df = list(self.qresults.values())[0]

        df.reset_index(level=0, inplace=True)

        # remove day from time data
        df["index"] = df["index"].map(lambda x: x.strftime("%H:%M"))

        # remove last row
        df.drop(df.tail(1).index, inplace=True)

        # convert to dictionary
        df.set_index("index", inplace=True)

        #invert data
        if(self.invert == True):
            df["mean"]  = df["mean"]  * -1

        ret = df.to_dict().get("mean")
        return ret

class DataQueryMQTT(DataQuery):
    def __init__(self, influxConnection: InfluxConnection,
                        power_column: str,
                        device: str,
                        tablename: str,
                        duration = GlobalConfig.sim_duration,
                        start = GlobalConfig.start_date,
                        interval = GlobalConfig.slot_length.in_minutes(),
                        invert=False):
        super().__init__(influxConnection, invert)

        end = start + duration
        qstring = f'SELECT mean("{power_column}") FROM "{tablename}" WHERE "device" =~ /^{device}$/ AND time >= \'{start.to_datetime_string()}\' AND time <= \'{end.to_datetime_string()}\' GROUP BY time({interval}m) fill(0)'
        self.set(qstring)