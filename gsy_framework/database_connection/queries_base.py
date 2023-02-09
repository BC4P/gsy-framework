from gsy_framework.database_connection.connection import Connection
from gsy_framework.constants_limits import GlobalConfig
from pendulum import duration
import pandas as pd
import os
import pathlib

# abstract base query class
class Query:
    def __init__(self, connection: Connection, duration, start, interval):
        self.connection = connection
        self._set_time(duration, start, interval)
        self.query_string()

    def _set_time(self, duration, start, interval):
        self.duration = duration
        self.start = start
        self.interval = interval
        self.end = self.start + self.duration

    def update_query(self, 
                duration = duration(days=1),
                start = GlobalConfig.start_date,
                interval = GlobalConfig.slot_length.in_minutes()):
        self._set_time(duration, start, interval)
        self.query_string()
    
    #executes query
    def exec(self):
        self.qresults = self.connection.query(self.qstring)
        return self.transform()

    def get_query_string(self):
        return self.qstring
    
    # defines query string: overwrite this class
    def query_string(self):
        self.qstring = ""

    # transforms results of query to dict class, that gets returned: overwrite this class
    def transform(self):
        return self.qresults


# raw query class (overwriting query string and transformation fucntion with a provided one)
class QueryRaw(Query):
    def __init__(self, connection: Connection, querystring: str, transform):
        super().__init__(connection, duration(days=1), GlobalConfig.start_date, GlobalConfig.slot_length.in_minutes())
        self.qstring = querystring
        self.transform = transform

    def exec(self):
        qresults = self.connection.query(self.qstring)
        return self.transform(qresults)