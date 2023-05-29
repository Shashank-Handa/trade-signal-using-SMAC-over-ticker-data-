import datetime
import matplotlib.pyplot as plt
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

    def twoTimeFrameCrossoverPredictor(self, instrumentName, timeFrame1, timeFrame2, startDateAndTime="2000-01-01 00:00:00", showPlot=False):
        returnVal=[]
        if(timeFrame1<timeFrame2):
            timeFrameLong = timeFrame2
            timeFrameShort = timeFrame1
        else:
            timeFrameLong = timeFrame1
            timeFrameShort = timeFrame2
        if(self.dbConnection):
            batchNumber = 0
            while(True):
                df=None
                with self.dbConnection.cursor() as cur:
                    sql = "SELECT dateAndTime, closePrice FROM tickerdata WHERE instrument=%s AND dateAndTime > %s LIMIT %s, 1000"
                    cur.execute(sql, (instrumentName, startDateAndTime, batchNumber*self.batchSize))
                    df = pandas.DataFrame(cur.fetchall(), columns=["dateAndTime", "closingPrice"])
                slowSMA = df.rolling(timeFrameLong, on="dateAndTime").mean().dropna()
                fastSMA = df.rolling(timeFrameShort, on="dateAndTime").mean().dropna()



                combinedDf = pandas.merge(slowSMA, fastSMA, on="dateAndTime")
                flag=-1
                for row in combinedDf.iterrows():
                    if row[1]["closingPrice_x"]<row[1]["closingPrice_y"] and flag!=0:
                        flag=0
                        returnVal.append(str(row[1]) + " \nBUY\n")
                    elif row[1]["closingPrice_x"]>row[1]["closingPrice_y"] and flag!=1:
                        flag=1
                        returnVal.append(str(row[1]) + " \nSELL\n")
                    else:
                        pass

                if(len(df.index)<self.batchSize):
                    break
        if(showPlot):
            ax=plt.gca()
            combinedDf.plot(kind='line',x="dateAndTime", y="closingPrice_y", color="red", ax=ax)
            combinedDf.plot(kind='line',x="dateAndTime", y="closingPrice_x", color="blue", ax=ax)
            plt.show()
        return returnVal


