import datetime

import pandas
import pymysql

class SMACpredictor:
    def __init__(self):
        self.dbConnection = None
        self.movingAverages = None
        self.batchSize = 5000

    def setConnection(self, given):
        if(not isinstance(given, pymysql.connections.Connection)):
            raise ValueError("argument not a pymysql.connections.Connection object")
        self.dbConnection = given

    def setBatchSize(self, batchSize):
        self.batchSize = batchSize

    def twoTimeFrameCrossoverPredictor(self, instrumentName, timeFrame1, timeFrame2, startDateAndTime="2000-01-01 00:00:00"):
        returnVal=[]
        if(self.dbConnection):
            batchNumber = 0
            while(True):
                df=None
                with self.dbConnection.cursor() as cur:
                    sql = "SELECT dateAndTime, closePrice FROM tickerdata WHERE instrument=%s AND dateAndTime > %s LIMIT %s, 1000"
                    cur.execute(sql, (instrumentName, startDateAndTime, batchNumber*self.batchSize))
                    df = pandas.DataFrame(cur.fetchall(), columns=["dateAndTime", "closingPrice"])
                slowSMA = df.rolling(timeFrame1, on="dateAndTime").mean()
                fastSMA = df.rolling(timeFrame2, on="dateAndTime").mean()
                crossoverpoints = pandas.merge(slowSMA, fastSMA)
                for row in crossoverpoints.iterrows():
                    trend = fastSMA[(fastSMA==row).shift(-1)]
                    if(trend["closingPrice"]>row["closingPrice"]):
                        returnVal.append(str(row) + "buy")
                    else:
                        returnVal.append(str(row) + "sell")
                if(len(df.index)<self.batchSize):
                    break
        return returnVal


