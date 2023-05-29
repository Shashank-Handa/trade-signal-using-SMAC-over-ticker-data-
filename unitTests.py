import DBInput
import unittest
import pymysql
import datetime

class TestDBService(unittest.TestCase):


    def test_insert(self):
        connection = pymysql.connect(
            host='localhost',
            user='root',
            password='H@rd.Study123$',
            database='stockdata'
        )
        myInserter = DBInput.DBInserter()
        myInserter.setConnection(connection)
        #test datetime validation (input has error in 1st parameter dateAndTime)
        self.assertRaises(ValueError, lambda: myInserter.insertTicker("2014-23-23 00:00:00","152.05","156.95", "149.7","156.95","19542712","HINDALCO"))
        #test price argument validation (input has error in 2nd parameter closePrice)
        self.assertRaises(ValueError, lambda: myInserter.insertTicker("2014-12-23 00:00:00","152.0593","156.95", "149.7","156.95","19542712","HINDALCO"))
        #test price argument validation(input has error in 3rd parameter openPrice)
        self.assertRaises(ValueError, lambda: myInserter.insertTicker("2014-12-23 00:00:00","152.05","1d6.95", "149.7","156.95","19542712","HINDALCO"))
        #test volume argument validation ( input has error in 6th parameter)
        self.assertRaises(ValueError, lambda: myInserter.insertTicker("2014-12-23 00:00:00","152.0593","1d6.95","149.7","156.95","19542712.10","HINDALCO"))
        #possible test for instrument name validation

        #test correct working
        self.assertEqual(myInserter.insertTicker("2014-05-23 00:00:00","152.05","156.95", "149.7","156.95","19542712","HINDALCO"), 1)
        self.assertEqual(myInserter.insertTicker(datetime.datetime.strptime("2014-05-23 00:00:01", '%Y-%m-%d %H:%M:%S'),"152.05","156.95", "149.7","156.95","19542712.00","HINDALCO"), 1)

if __name__ == '__main__':
    unittest.main()