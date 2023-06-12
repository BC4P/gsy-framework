from gsy_framework.database_connection.connection import Connection, PostgreSQLConnection
from gsy_framework.database_connection.queries_base import Query
from gsy_framework.constants_limits import GlobalConfig
from pendulum import duration
import os
import pathlib

class QueryPostgresSQL(Query):
    def __init__(self, connection: PostgreSQLConnection, duration, start, interval, multiplier):
        self.multiplier = multiplier
        super().__init__(connection, duration, start, interval)
    
    def transform(self):
        if(len(self.qresults) == 0):
            print("Load Profile for Query:\n" + self.qstring + "\nnot valid. Using Zero Curve.")
            return os.path.join(pathlib.Path(__file__).parent.resolve(), "resources", "Zero_Curve.csv")

        dic = {k.strftime("%H:%M"):v*1000.0*self.multiplier for k,v in self.qresults}
        return dic