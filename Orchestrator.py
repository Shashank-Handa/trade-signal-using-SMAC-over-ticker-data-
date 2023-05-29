import openpyxl
import DBInput
import pymysql
import SMAC

connection = pymysql.connect(
            host='localhost',
            user='root',
            password='H@rd.Study123$',
            database='stockdata'
        )
myInserter = DBInput.DBInserter()
myInserter.setConnection(connection)

wb = openpyxl.load_workbook("resources/HINDALCO_1D.xlsx")
ws = wb.active

#for row in ws.iter_rows(min_row=2, max_row=70, values_only=True):
#    myInserter.insertTicker(*row)

myPredictor = SMAC.SMACpredictor()
myPredictor.setConnection(connection)

result = myPredictor.twoTimeFrameCrossoverPredictor(5, 30)
print(result)




