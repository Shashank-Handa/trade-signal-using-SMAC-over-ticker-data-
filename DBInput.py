import datetime
import decimal
import re
import pymysql

class DBInserter():
    def __init__(self):
        self.dbConnection = None

    def setConnection(self, given):
        if(not isinstance(given, pymysql.connections.Connection)):
            raise ValueError("argument not a pymysql.connections.Connection object")
        self.dbConnection = given

    def verifyInstrumentName(self, name):
        #logic to validate instrument name according to specific market's standards
        return True

    def insertTicker(self, dateAndTime, closePrice, openPrice, highPrice, lowPrice, volume, instrument):
        if(not isinstance(dateAndTime, datetime.datetime)):
            try:
                dateAndTime = datetime.datetime.strptime(dateAndTime, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                raise ValueError("dateAndTime parameter should be in format 'YYYY-MM-DD H:M:S' and should be a valid date")

        if (not (re.match(r'^[1-9]\d*(\.\d{1,2})?$', str(closePrice)) and
                re.match(r'^[1-9]\d*(\.\d{1,2})?$', str(openPrice)) and
                re.match(r'^[1-9]\d*(\.\d{1,2})?$', str(highPrice)) and
                re.match(r'^[1-9]\d*(\.\d{1,2})?$', str(lowPrice)))):
            raise ValueError("price argument (closePrice, openPrice, hihgPrice, lowPrice) is not of currency format (decimal with scale 2)")

        if (not re.match(r'^[1-9]\d*(\.\d{1,2})?$', str(closePrice))):
            raise ValueError("closePrice argument is not of currency format (decimal with scale 2)")

        if (not re.match(r'^[1-9]\d*(\.\d{1,2})?$', str(highPrice))):
            raise ValueError("highPrice argument is not of currency format (decimal with scale 2)")
        if (not re.match(r'^[1-9]\d*(\.\d{1,2})?$', str(lowPrice))):
            raise ValueError("lowPrice argument is not of currency format (decimal with scale 2)")
        if (not re.match(r'^[1-9]\d*(\.0*)?$', str(volume))):
            raise ValueError("volume argument is not an integer")
        if (not self.verifyInstrumentName(instrument)):
            raise ValueError("Invalid instrument name")

        returnVal = None
        if (self.dbConnection):
            with self.dbConnection.cursor() as cursor:
                sql = "INSERT INTO `tickerdata` (`dateAndTime`, `closePrice`, `openPrice`, `highPrice`, `lowPrice`, `volume`, `instrument`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                returnVal = cursor.execute(sql, (dateAndTime.strftime('%Y-%m-%d %H:%M:%S'), closePrice, openPrice, highPrice, lowPrice, volume, instrument))
            self.dbConnection.commit()
        else:
            raise Exception("No database connection given")

        return returnVal








