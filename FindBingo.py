# -*- coding: utf-8 -*-
""" NoAskingLa 2017-08-26 """
import csv
import pandas as pd
import os
import sqlite3
import datetime
import xlrd
import TechnicalAnalysisIndicators as TAI
import TrailingStop as TS
import numpy as np
import time
from datetime import timedelta
import requests
import sys
import shutil
import PyWinDDE
DBpath = 'Local.db'
RDBpath = 'Realtime.db'
Realfile = 'GetReal.xls'
BingoPath = 'C:\\Users\dhuang01\\Desktop\\Print\\tsec-master\\Bingo'

VolumeOffset = {'12:05': 1.42, '11:25': 1.66, '12:00': 1.45, '11:20': 1.7, '12:40': 1.28, '10:45': 2.01, '12:45': 1.26, '09:40': 3.7, '10:50': 1.95, '10:55': 1.89,
                '09:45': 3.5, '11:00': 1.85, '12:25': 1.34, '11:05': 1.81, '12:20': 1.36, '09:25': 5.0, '09:20': 6.0, '10:15': 2.5, '09:05': 15.0, '10:30': 2.22,
                '10:10': 2.6, '10:35': 2.15, '13:10': 1.14, '13:05': 1.17, '11:30': 1.63, '13:20': 1.08, '13:25': 1.05, '11:35': 1.6, '12:55': 1.22, '12:50': 1.24,
                '13:15': 1.11, '10:25': 2.3, '10:40': 2.08, '11:55': 1.48, '10:20': 2.4, '11:50': 1.51, '09:50': 3.3, '12:35': 1.3, '11:10': 1.77, '12:10': 1.4,
                '12:30': 1.32, '12:15': 1.38, '11:15': 1.74, '09:35': 4.0, '11:45': 1.54, '13:30': 1.0, '09:10': 10.0, '09:30': 4.5, '09:15': 8.0, '10:05': 2.7,
                '09:55': 3.1, '10:00': 2.9, '13:00': 1.2, '11:40': 1.57}
CalculatedColumnList=["TradeDate",
                 "W_Zero","W_Open","W_Close","W_High","W_Low","W_Volume","W_Value",
                 "D_K_9","D_D_9","D_RSV_9",
                 "D_NK_9","D_ND_9","D_NRSV_9",
                 "D_DIFF_12_26","D_MACD_9","D_OSC",
                 "D_NDIFF_12_26","D_NMACD_9","D_NOSC",
                 "D_Pressure_120_5","W_Pressure_96_10",
                 "D_Bias_5","D_Bias_10","D_Bias_20","D_Bias_60",
                 "W_Bias_4","W_Bias_13","W_Bias_26","W_Bias_52",
                 "D_BiasDiff_5_10","D_BiasDiff_5_20","D_BiasDiff_5_60","D_BiasDiff_10_20","D_BiasDiff_10_60","D_BiasDiff_20_60",
                 "D_MoM_10","D_MoMA_10"]
BingoColumnList = ['TradeDate','FI_IV_Ratio','MF1day','MF5day','MF10day','MF20day','MF60day','FI','FI10day','IV','IV10day','Bingo','TotalCapital']
InsertColumnList = ["TradeDate","Zero","Open","Close","High","Low","Volume","Value","TradeCount"]
CheckColumnList = CalculatedColumnList+BingoColumnList+InsertColumnList
UpdateSQL = """
            UPDATE [tblname] SET [InsertData] WHERE [TradeDate];
            """
InsertSQL = """
                INSERT OR IGNORE INTO [tblname]([ColumnName])
                VALUES([InsertValue])
            """
def DBS(tblname,tblList=[]):
    if  tblname == 'tblRecordHistory':
        c.execute('''CREATE TABLE IF NOT EXISTS  tblRecordHistory
           (FunctionName TEXT PRIMARY KEY     NOT NULL,
               LastPK          INT    NOT NULL);''')
        tblList.append(tblname)
    else:
        c.execute("""CREATE TABLE IF NOT EXISTS [tblname] (
           TradeDate INT PRIMARY KEY     NOT NULL,
            Zero   REAL,
            Open           REAL,
            Close           REAL,
            High           REAL,
            Low           REAL,
            Volume           INT,
            Value           REAL,
            TradeCount           INT)
            ;""".replace('[tblname]',tblname))
        tblList.append(tblname)
    return tblList

            
class CrawlData(object):
    def __init__(self, tblList = []):
        self.tblList = tblList
        self.LastUpdate = 20171226
        cursor = c.execute("""SELECT name FROM sqlite_master WHERE type='table';""")
        for row in cursor:
            self.tblList.append(str(row[0]))
        if datetime.datetime.now().hour in range(9,13):
            print datetime.datetime.now(),'self.GetRealTimeData()'
            self.GetRealTimeData()
        else:            
            if 'tblRecordHistory' not in tblList:
                self.tblList = DBS('tblRecordHistory',tblList)
            cursor = c.execute("""SELECT LastPK FROM tblRecordHistory WHERE FunctionName = 'CrawlHistory';""")
            for row in cursor:
                self.LastUpdate = row[0]
            reload(sys)                        
            sys.setdefaultencoding('utf-8')
            startTime = datetime.datetime.strptime(str(self.LastUpdate),'%Y%m%d')
##            startTime = datetime.datetime(2016,8,17)
            stopTime =  datetime.datetime.now()
            while startTime < datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time()):
                startTime += datetime.timedelta(days=1)
                self.getTSE_Profile(startTime)
                self.getOTC_Profile(startTime)
                print 'Time elapsed:',datetime.datetime.now()-stopTime
                stopTime =  datetime.datetime.now()
                time.sleep(2)
            c.execute(''' INSERT OR REPLACE INTO tblRecordHistory (FunctionName,LastPK)
                      VALUES('CrawlHistory',[LastPK]) '''.replace('[LastPK]',str(self.LastUpdate)))
        self.CheckExpireTable()
        
    def InsertStockData(self,tblname,InsertValue): #[TradeDate,Zero,Open,Close,High,Low,Volume,Value,TradeCount]
        InsertData = []
        for d in range(len(InsertColumnList)):
            InsertData.append(InsertColumnList[d] + ' = ' + ("%.2f" % InsertValue[d]))
        c.execute(InsertSQL.replace('[tblname]',tblname).replace('[InsertValue]',','.join(str(s) for s in InsertValue)).replace('[ColumnName]',','.join(s for s in InsertColumnList)))
        c.execute(UpdateSQL.replace('[tblname]',tblname).replace('[TradeDate]', InsertData[0]).replace('[InsertData]',','.join(str(s) for s in InsertData[1:])))
        if InsertValue[0] > self.LastUpdate:
            self.LastUpdate = InsertValue[0]
                        
    def getTSE_Profile(self,qdate):
        # 成交資訊
        urlname = 'http://www.twse.com.tw/exchangeReport/MI_INDEX'
        query_params = {'date': qdate.strftime('%Y%m%d'),'response': 'json','lang':'zh','type': 'ALL','_': str(round(time.time() * 1000) - 500)}
        TradeDate = qdate.year*10000 + qdate.month*100 + qdate.day 
        print urlname,query_params
        # Get json data
        res = requests.get(urlname, params=query_params)
        pds = res.json()
        tablelist = [tbl for tbl in list(pds) if tbl.startswith('data')]
        # ID,name,volume,tradecount,value,open,high,low,close,+/-,range,
        for tbl in list(pds):
            if tbl.startswith('data'):
                for row in pds[tbl]:
                    if len(row[0]) == 4 :
                        Insertrow = [s.replace(u'-',u'0') for s in row]
                        tblname = 'tbl' + str(Insertrow[0])
                        if tblname  not in tblList:
                            DBS(tblname,tblList)
                        Close = float(Insertrow[8].replace(',',''))
                        Open = float(Insertrow[5].replace(',',''))
                        High = float(Insertrow[6].replace(',',''))
                        Low = float(Insertrow[7].replace(',',''))
                        Volume = int(Insertrow[2].replace(',',''))
                        Value = float(Insertrow[4].replace(',',''))
                        TradeCount = int(Insertrow[3].replace(',',''))
                        if Close == 0.0:
                            Close = float(Insertrow[11].replace(',',''))
                            Open = float(Insertrow[11].replace(',',''))
                            High = float(Insertrow[11].replace(',',''))
                            Low = float(Insertrow[11].replace(',',''))
                        if u'color:green' in Insertrow[9]:
                            Range = float(Insertrow[10].replace(',','')) * (-1.0)
                        else:
                            Range = float(Insertrow[10].replace(',',''))
                        Zero = Close - Range
                        InsertValue = [TradeDate,Zero,Open,Close,High,Low,Volume,Value,TradeCount]
                        self.InsertStockData(tblname,InsertValue)
        conn.commit()
        
    def GetRealTimeData(self):
        def round_minutes(dt, direction, resolution):
            new_minute = (dt.minute // resolution + (1 if direction == 'up' else 0)) * resolution
            return dt + datetime.timedelta(minutes=new_minute - dt.minute)
        Rconn = sqlite3.connect(RDBpath, check_same_thread=False)
        Rc = Rconn.cursor()
        Rtblname = datetime.datetime.now().strftime('tbl%Y%m%d')
        TradeDate = int(datetime.datetime.now().strftime('%Y%m%d'))
        VolTimes = VolumeOffset[round_minutes(datetime.datetime.now(), 'up', 5).strftime('%H:%M')]
        Rcursor = Rc.execute('select ID, Zero,Open,Close,High,Low,Volume,Value from [tblname]'.replace('[tblname]',Rtblname))
        TradeCount = 0
        for row in Rcursor:
            tblname = 'tbl' + str(row[0]).zfill(4)
            InsertValue = [TradeDate] + list(row)[1:] + [TradeCount]
            InsertValue[-2] = InsertValue[-2] *VolTimes
##            InsertValue[-3] = InsertValue[-3] *VolTimes
            self.InsertStockData(tblname,InsertValue)
        conn.commit()
        Rconn.close()

    def getOTC_Profile(self,qdate):
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0'} 
        rs = requests.session()
        QuerDate = str(qdate.year-1911) + '/' + qdate.strftime('%m/%d')
        TradeDate = qdate.year*10000 + qdate.month*100 + qdate.day 
        urlname = 'http://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_print.php?l=zh-tw&d=[QuerDate]&s=0,asc,0'.replace('[QuerDate]',QuerDate)
        print urlname
        res = requests.get(urlname,headers=headers)
        pds = pd.read_html(res.text)[0]
        np_df = pds.values
        StockList = []
        datalist = []
        # 1333, 恩得利, close, difference, Open, High ,Low, Average, Volume, Value, tradeCount,LastBuy,LastSell, TotalVolume, Next Open, Next High, Next Low 
        for row in np_df:
            if len(row[0]) == 4 and type(row[0]) == str:
                tblname = 'tbl' + str(row[0])
                if tblname not in tblList:
                    DBS(tblname,tblList)
                Insertrow = []
                for col in row:
                    if type(col) == str:
                        Insertrow.append(col.replace('+',''))
                    else:
                        Insertrow.append(col)
                if u'\u9664' in row[3]:
                    Insertrow[3] = '0'
                if '---' in Insertrow[2]:
                    Insertrow[2] = Insertrow[7]
                    Insertrow[4] = Insertrow[7]
                    Insertrow[5] = Insertrow[7]
                    Insertrow[6] = Insertrow[7]
                if '---' in Insertrow[3]:
                    Insertrow[3] = '0'
                Close = float(Insertrow[2])
                Zero = float(Insertrow[2])-float(Insertrow[3])
                Open = float(Insertrow[4])
                High = float(Insertrow[5])
                Low = float(Insertrow[6])
                Volume = int(Insertrow[8])
                Value = float(Insertrow[9])
                TradeCount = int(Insertrow[10])
                InsertValue = [TradeDate,Zero,Open,Close,High,Low,Volume,Value,TradeCount]
                self.InsertStockData(tblname,InsertValue)
        conn.commit()

    def CheckExpireTable(self):
        tblList = []
        Expired = int((datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y%m%d'))
        for tblname in sorted(self.tblList):
            if len(tblname) == 7:
                c.execute("""delete from [tblname] where rowid = (SELECT rowid FROM [tblname] WHERE TradeDate > 99999999)""".replace('[tblname]',tblname))
                cursor = c.execute("""SELECT max(TradeDate) FROM [tblname] """.replace('[tblname]',tblname))
                for row in cursor:
                    if row[0] < Expired :
                        print datetime.datetime.now(),row[0],Expired,tblname
                        c.execute("""DROP TABLE [tblname];""".replace('[tblname]',tblname))
                        self.tblList.remove(tblname)
        conn.commit() 

        
def GetBingoData():
    csvrow = []
    CodeList = {u'代號':0,u'土洋佔比':1,u'近1日籌碼集中度':2,u'近5日籌碼集中度':3,u'近10日籌碼集中度':4,u'近20日籌碼集中度':5,u'近60日籌碼集中度':6,
                u'外資買賣超':7,u'外資近10日買賣超':8,u'投信買賣超':9,u'投信近10日買賣超':10,u'突破':11,u'股本(億)':12}
    CodeOrder = {}
    for file in os.listdir(BingoPath):
        if file.endswith('.xls'):
            print file
            filepath = os.path.join(BingoPath,file)
            fileDate= datetime.datetime.strptime(file.replace('.xls',''),'%Y%m%d')
            if fileDate <= datetime.datetime.now():
                fileDate = fileDate.strftime('%Y%m%d')
                ad_wb = xlrd.open_workbook(filepath)
                sheet_0 = ad_wb.sheet_by_index(0)
                for i in range(sheet_0.ncols) : 
                    if (sheet_0.cell_value(0, i)) in CodeList:
                        CodeOrder[i] = (sheet_0.cell_value(0, i))
                for j in range(1,sheet_0.nrows):
                    InsertValue = ['' for col in range(len(CodeList))]
                    for i in range(sheet_0.ncols) :
                        if i in CodeOrder:
                            CodeIndex = CodeList[CodeOrder[i]]
                            if CodeIndex == 11:
                                if str(sheet_0.cell_value(j,i)) == 'Checked':
                                    InsertValue[CodeIndex] = 1
                                else:
                                    InsertValue[CodeIndex] = 0
                            else:
                                if ' ' in str(sheet_0.cell_value(j,i)):
                                    InsertValue[CodeIndex] = 0.0
                                else:
                                    if str(sheet_0.cell_value(j,i)):
                                        InsertValue[CodeIndex] = float(str(sheet_0.cell_value(j,i)).replace(',',''))
                                    
                    if InsertValue[1]:
                        tblname = 'tbl' + str(int(InsertValue[0]))
                        if len(tblname) == 7 and tblname in tblList:
                            TradeDate = int(fileDate)
                            InsertValue[0] = TradeDate
                            InsertData = []
                            for d in range(len(BingoColumnList)):
##                                print BingoColumnList[d]+":'" + str(InsertValue[d])+"'",
                                InsertData.append(BingoColumnList[d] + ' = ' + ("%.2f" % InsertValue[d]))
##                            print ''
                            c.execute(UpdateSQL.replace('[tblname]',tblname).replace('[TradeDate]', InsertData[0]).replace('[InsertData]',','.join(str(s) for s in InsertData[1:])))
                conn.commit()
                shutil.move(os.path.join(BingoPath,file), os.path.join(os.path.join(BingoPath,'Archive'),file))

def CheckSellList(filename,Method,BasicBuy = 100000,BackTest = True,DrawChart = False ,outputpath = ''): # tblname,TradeDate,BuyPrice
    Header =    ['ID','TradeDate','Close','Raise']
    Header +=   ['FailCount','FailItem']
    Header +=   ['BingoCount','FI_IV_Ratio','Bingo']
##    Header += ['TotalCapital', 'D_Pressure_120_5', 'W_Pressure_96_10', 'BiasDiff_B5', 'D_BiasDiff_B20', 'D_Bias_A20', 'W_Bias_A50', 'D_KbD',
##               'D_K_9_Delta', 'D_K_9', 'DN_KbD', 'DN_K_9_Delta', 'DN_K_9', 'D_OSC_Delta', 'D_OSC_1', 'D_OSC', 'D_DIFF', 'DN_OSC_Delta', 'DN_DIFF',
##               'Value', 'Value1', 'Volume_Time','D_MoM_10', 'D_MoM_10_Delta', 'LH', 'RaiseB', 'TR','DK','VOSC','VDIFF','VMACD','VOSC_Gain']
    Header += ['TotalCapital', 'D_Pressure_120_5', 'W_Pressure_96_10', 'BiasDiff_B5', 'D_BiasDiff_B20', 'D_Bias_A20', 'W_Bias_A50', 'KD_Delta',
       '"D_K_9_Gain', 'D_K_9', 'D_NKD_Delta', 'D_K_9_Gain', 'DN_K_9', 'OSC_Gain', "D_OSC", 'D_DIFF', 'DN_OSC_Delta', 
       'Value', 'Value1', 'Volume_Time','D_MoM_10','D_MoM_Delta', "D_MoM_Gain", 'LH', 'RaiseB', 'TR','DK','VOSC','VDIFF','VMACD','VOSC_Gain']
    TSCol = ['Comment', 'SellDate', 'SellPrice', 'MS', 'MSCount','estGain']
    for method in sorted(Method):
        Header +=[method+'_'+s for s in  TSCol]
    sqlString ="""
                    select TradeDate,Zero,Open,Close,High,Low,Volume,Value,D_K_9,D_D_9,D_OSC,D_MACD_9,D_DIFF_12_26,D_OSC
                    from [tblname]
                    order by TradeDate desc
                    limit 500
                """
    fp = open(filename,'rb')
    reader = csv.reader(fp)
    QueryList = {}
    reader.next()
    for row in reader:
##        print row
        if int(row[4]) <1:
            tblname = 'tbl'+ row[0]
            TradeDate = int(row[1])
            BuyPrice = float(row[2])
            if tblname not in QueryList:
                QueryList[tblname] = []
            QueryList[tblname].append([TradeDate,BuyPrice]+row[3:])
    fp.close()
    csvrow = []
    sc = 0
    for tblname in sorted(QueryList):
        sc += 1
        print '\r [%-20s] %d%%' % ('='*int(20*sc/len(QueryList)), 100*sc/len(QueryList)),datetime.datetime.now(),tblname,
        df = pd.read_sql_query(sqlString.replace('[tblname]',tblname), conn)
        df = df.sort_values(by='TradeDate', ascending=True)
        df.reset_index(inplace=True)
        df['MA_5'] = TAI.MA(df['Close'],5)
        StockHis = {'Buy':[],'Sell':{}}
        Result = {}
        TradeList = []
        Data = {}
        for row in QueryList[tblname]:
            TradeList.append(row[0])
            Data[row[0]] = row
        if 'DongDa' in Method:
            Result['DongDa'] = TS.DongDa(df,TradeList[:],BackTest)
        if 'WDa' in Method: 
            Result['WDa'] = TS.WDa(df,TradeList[:],BackTest)
        if 'SAR' in Method:
            Result['SAR'] = TS.SAR(df,TradeList[:], 0.02, 0.2,BackTest)
        if 'KeyKbar' in Method:
            Result['KeyKbar'] = TS.KeyKbar(df,TradeList[:],BackTest)

        for method in sorted(Result):
            for SellInfo in Result[method]:
                for TradeDate in SellInfo[-1]:
                    if TradeDate in Data:
                        appendList = SellInfo[:-1]
                        appendList[1] = datetime.datetime.strptime(str(appendList[1]),'%Y%m%d').strftime('%Y-%m-%d')
                        Data[TradeDate] +=  appendList
                        StockHis['Buy'].append(TradeDate)
                        if SellInfo[0] == 'Sell':
                            if SellInfo[1] not in StockHis['Sell']:
                                StockHis['Sell'][SellInfo[1]] = []
                            if method not in StockHis['Sell'][SellInfo[1]]:
                                StockHis['Sell'][SellInfo[1]].append(method)
                    else:
                        print tblname,TradeDate
        for TradeDate in Data:
            Data[TradeDate][0] = datetime.datetime.strptime(str(Data[TradeDate][0]),'%Y%m%d').strftime('%Y-%m-%d')
            csvrow.append([tblname.replace('tbl','')]+Data[TradeDate])
        if DrawChart :
            TS.DrawChart(df,tblname,StockHis,outputpath,ChartLen = 180)
    fp = open(filename.replace('.csv','Result.csv'),'wb')
    fout = csv.writer(fp)
    fout.writerow(Header)
    for row in csvrow:
        fout.writerow(row)
    fp.close()


def CheckColumnExist(tblname,colList):
    cursor = c.execute("""
                        PRAGMA table_info([tblname]);
                        """.replace('[tblname]',tblname))
    ExistColList,NewColList = [],[]
    for row in cursor:
        if row[1] not in ExistColList:
            ExistColList.append(row[1])
    for colname in colList:
        if colname not in ExistColList:
            NewColList.append(colname)
            c.execute("""
                        ALTER TABLE [tblname] ADD COLUMN [colname] REAL ;
                        """.replace('[tblname]',tblname).replace('[colname]',colname))
    conn.commit()
    return NewColList

def CalculateData(tblname,N = 0):
    NullList = []
    QueryEnable = True
    if N == 1:
        cursor = c.execute("""
                        select TradeDate
                        from [tblname]
                        where D_K_9 is null
                        order by TradeDate desc
                        limit 1
                    """.replace('[tblname]',tblname))
        for row in cursor:
            N += 1
            NullList.append(datetime.datetime.strptime(str(row[0]),'%Y%m%d'))
    if N <= 1:
        sqlString ="""
                    select TradeDate,Zero,Open,Close,High,Low,Volume,Value
                    from [tblname]
                    order by TradeDate desc
                    limit 500
                """
        N = 1
    else:
        sqlString ="""
                    select TradeDate,Zero,Open,Close,High,Low,Volume,Value
                    from [tblname]
                    order by TradeDate desc
                """
    df = pd.read_sql_query(sqlString.replace('[tblname]',tblname), conn)
    if df['Volume'][0] < 50:
        QueryEnable = False
    if QueryEnable:
        df['TradeDate'] = pd.to_datetime(df['TradeDate'].astype(str), format='%Y%m%d')
        df = df.sort_values(by='TradeDate', ascending=True)
        for n in [5,10,20,60,120,240]:
            df['MA_'+str(n)] = TAI.MA(df['Close'],n)
        df = TAI.MACD(df, 12, 26)
        df = TAI.KD(df, 9)
        df['MoM_10'] = TAI.MOM(df, 10)
        df['MoMA_10'] = TAI.MA(df['MoM_10'], 10)
        df = TAI.GetBiasDiff(df,[5,10,20,60])
        df = TAI.GetBias(df,[5,10,20,60])
        df['Median'] = pd.eval('(df.High + df.Low)/2')
        df = df.sort_values(by='TradeDate', ascending=False)
        if NullList:
            N = df.index[-1]
        for ts in range(N):
            df.reset_index(inplace=True,drop=True)
            if ts == 0 or df['TradeDate'][0] in NullList:
                wdf = ''
                wdf = TAI.ConvertFrequency(df,period_type = 'W')
                wdf = wdf.sort_values(by='TradeDate', ascending=True)
                wdf['Median'] = pd.eval('(wdf.High + wdf.Low)/2')
                for n in [4,13,26,52,104]:
                    wdf['MA_'+str(n)] = TAI.MA(wdf['Close'],n)
                wdf = TAI.GetBias(wdf,[4,13,26,52])
                pressure = TAI.GetPressure(df,Kbar=120,Gridpercent= 5.0,testBar = 0)
                wpressure = TAI.GetPressure(wdf,Kbar=96,Gridpercent= 10.0,testBar = 0)
                NextRSV = 100.0*(df['Close'][0] - min(df['Low'][:8]))/(max(df['High'][:8]) - min(df['Low'][:8]))
                NextK = 1.0/3.0*df['K_9'][0] + 2.0/3.0*NextRSV
                NextD = 1.0/3.0*df['D_9'][0] + 2.0/3.0*NextK            
                NextEMA12 = (sum(df['High'][:11]) + sum(df['Low'][:11]) + 2.0*sum(df['Close'][:11]))/12.0/4.0*11.0/13.0 + df['Close'][0]*2.0/13.0
                NextEMA26 = (sum(df['High'][:25]) + sum(df['Low'][:25]) + 2.0*sum(df['Close'][:25]))/26.0/4.0*25.0/27.0 + df['Close'][0]*2.0/27.0
                NextDif = (NextEMA12 / NextEMA26 - 1.0)*100 
                NextMACD = df['DIFF_12_26'][0] *8.0/10.0 + NextDif * 2.0/10.0
                NextOSC = NextDif  - NextMACD
                InsertValue = [int(str(df["TradeDate"][0]).split(' ')[0].replace('-','')),
                                 wdf["Zero"][0],wdf["Open"][0],wdf["Close"][0],wdf["High"][0],wdf["Low"][0],wdf["Volume"][0],wdf["Value"][0],
                                 df["K_9"][0],df["D_9"][0],df["RSV_9"][0],
                                 NextK,NextD,NextRSV,
                                 df["DIFF_12_26"][0],df["MACD_9"][0],df["OSC"][0],
                                 NextDif,NextMACD,NextOSC,
                                 pressure,wpressure,
                                 df["Bias_5"][0],df["Bias_10"][0],df["Bias_20"][0],df["Bias_60"][0],
                                 wdf["Bias_4"][0],wdf["Bias_13"][0],wdf["Bias_26"][0],wdf["Bias_52"][0],
                                 df["BiasDiff_5_10"][0],df["BiasDiff_5_20"][0],df["BiasDiff_5_60"][0],df["BiasDiff_10_20"][0],df["BiasDiff_10_60"][0],df["BiasDiff_20_60"][0],
                                 df["MoM_10"][0],df["MoMA_10"][0]]
                InsertData = []
                for d in range(len(InsertValue)):
                    if np.isnan(InsertValue[d]):
                        InsertValue[d] = 9999999999
                    elif str(InsertValue[d]).lower() == 'inf':
                        InsertValue[d] = 9999999999
                    InsertData.append(CalculatedColumnList[d] + ' = ' + ("%.2f" % InsertValue[d]))

                c.execute(UpdateSQL.replace('[tblname]',tblname).replace('[TradeDate]', InsertData[0]).replace('[InsertData]',','.join(str(s) for s in InsertData[1:])))
                if df['TradeDate'][0] in NullList : NullList.remove(df['TradeDate'][0])

            if not NullList: break
    ##            except: print df
            df.drop(0, inplace=True)

def CheckBingoCriteria2(tblname, N = 1):
    ResultList = []
    BingoList = ['MF1day','MF5day','MF10day','MF20day','MF60day','FI','FI10day','IV','IV10day']
    BiasDiff = ["D_BiasDiff_5_10","D_BiasDiff_5_20","D_BiasDiff_5_60","D_BiasDiff_10_20","D_BiasDiff_10_60","D_BiasDiff_20_60"]
    sqlString ="""
                    select *
                    from [tblname]
                    order by TradeDate desc
                    limit [Count]
                """.replace('[Count]',str(N+500))
    df = pd.read_sql_query(sqlString.replace('[tblname]',tblname), conn)
    df = df.sort_values(by='TradeDate', ascending=True)
    df.reset_index(inplace=True,drop=True)
    df['D_MoM_9'] = TAI.MOM(df, 9)
    df = TAI.VMACD(df, 12, 26)
    df['VOSC_L3M'] = df["VOSC"].rolling(5).min()
    df['VOSC_Gain'] = df["VOSC"] - df['VOSC_L3M']
    df['OSC_L3M'] = df["D_OSC"].rolling(5).min()
    df['OSC_Gain'] = df["D_OSC"] - df['OSC_L3M']
    df["KD_Delta"] = df["D_K_9"] - df["D_D_9"]
    df["KD_Delta_L3M"] = df["KD_Delta"].rolling(5).min()
    df["KD_DeltaGain"] = df["KD_Delta"] - df["KD_Delta_L3M"]
    df["D_K_9_Gain"] = df["D_K_9"] - df["D_K_9"].rolling(5).min()
    df["D_NKD_Delta"] = df["D_NK_9"] - df["D_ND_9"]
    df["D_MoM_Delta"] = df['D_MoM_9']-df['D_MoM_10']
    df["D_MoM_Gain"] = df['D_MoM_10']-df['D_MoM_10'].rolling(5).min()
    df = df.sort_values(by='TradeDate', ascending=False)
    df.reset_index(inplace=True,drop=True)
    if df.index[-1] > N:
        for ts in range(N):
            Failcount = []
            Failitem = []
            if not (df["VOSC"][0]       > 0 and df['VOSC_L3M'][0]      <0 ):Failitem.append('VOSCX')
            if not (df["D_OSC"][0]      > 0 and df['OSC_L3M'][0]       <0 ):Failitem.append('OSCX')
            if not (df["KD_Delta"][0]   > 0 and df["KD_Delta_L3M"][0]  <0 ):Failitem.append('KDX')
            Failcount.append(df['TotalCapital'][0])  # <300
            Failcount.append(df['D_Pressure_120_5'][0])
            Failcount.append(df['W_Pressure_96_10'][0])
            BiasDiff = ["D_BiasDiff_5_10","D_BiasDiff_5_20","D_BiasDiff_5_60","D_BiasDiff_10_20","D_BiasDiff_10_60","D_BiasDiff_20_60"]
            Failcount.append(sum(1 if df[x][0] < 5  else 0 for x in BiasDiff))
            Failcount.append(sum(1 if df[x][0] < 20 else 0 for x in BiasDiff))
            Failcount.append(sum(1 if df[x][0] > 20 else 0 for x in ["D_Bias_5","D_Bias_10","D_Bias_20","D_Bias_60"]))
            Failcount.append(sum(1 if df[x][0] > 50 else 0 for x in ["W_Bias_4","W_Bias_13","W_Bias_26","W_Bias_52"]))
            Failcount.append(df["KD_Delta"][0])
            Failcount.append(df["D_K_9_Gain"][0])
            Failcount.append(df["D_K_9"][0])
            Failcount.append(df["D_NKD_Delta"][0])
            Failcount.append(df["D_K_9_Gain"][0]) #
            Failcount.append(df["D_NK_9"][0])
            Failcount.append(df['OSC_Gain'][0])   #
            Failcount.append(df["D_OSC"][0])
            Failcount.append(df["D_DIFF_12_26"][0])
            Failcount.append(df["D_NOSC"][0] - df["D_OSC"][0])   
            Failcount.append(df['Value'][0])
            Failcount.append(df['Value'][1])
            Failcount.append(float(df['Volume'][0]) /(sum(df['Volume'][1:6])/5)  ) # > 0.026
            Failcount.append(df["D_MoM_10"][0])
            Failcount.append(df["D_MoM_Delta"][0])
            Failcount.append(df["D_MoM_Gain"][0])

            Failcount.append((df['High'][0] - df['Close'][0])*100.0/df['Zero'][0])
            Failcount.append((df['Close'][0] - df['Open'][0])*100.0/df['Zero'][0] ) 
            Failcount.append((df['Close'][0]- df['Low'][0])*100.0/df['Zero'][0] )
            if df['Close'][0]- df['Low'][0] == 0:
                Failcount.append(100)
            else:
                Failcount.append((df['High'][0] - df['Close'][0])*100.0/(df['Close'][0]- df['Low'][0]))
            Failcount.append(df["VOSC"][0])
            Failcount.append(df["VDIFF"][0])
            Failcount.append(df["VMACD"][0])
            Failcount.append(df['VOSC_Gain'][0])
            BingoCount = sum(1 if df[x][0] > 0  else 0 for x in BingoList)   
            Result =    [tblname[3:],df['TradeDate'][0],df['Close'][0],100.0*df['Close'][0]/df['Zero'][0]-100]
##            Result +=   [len(Failitem),','.join(str(s) for s in Failitem)]
            Result +=   [len(Failitem),'FindBingo']
            Result +=   [BingoCount,df['FI_IV_Ratio'][0],df['Bingo'][0]]
            Result +=   Failcount
            ResultList.append(Result)
            df.drop(0, inplace=True)
            df.reset_index(inplace=True,drop=True)
    return ResultList

def Above5MA(tblname, N = 1):
    ResultList = []
    BingoList = ['MF1day','MF5day','MF10day','MF20day','MF60day','FI','FI10day','IV','IV10day']
    BiasDiff = ["D_BiasDiff_5_10","D_BiasDiff_5_20","D_BiasDiff_5_60","D_BiasDiff_10_20","D_BiasDiff_10_60","D_BiasDiff_20_60"]
    sqlString ="""
                    select *
                    from [tblname]
                    order by TradeDate desc
                    limit [Count]
                """.replace('[Count]',str(N+500))
    df = pd.read_sql_query(sqlString.replace('[tblname]',tblname), conn)
    df = df.sort_values(by='TradeDate', ascending=True)
    df.reset_index(inplace=True,drop=True)
    df = TAI.VMACD(df, 12, 26, nsig=9)
    df['P_5'] = TAI.MA(df['Close'],5)
    df['P_10'] = TAI.MA(df['Close'],10)
    df['P_20'] = TAI.MA(df['Close'],20)
    df['P_60'] = TAI.MA(df['Close'],60)
    df['P_120'] = TAI.MA(df['Close'],120)
    df['P_240'] = TAI.MA(df['Close'],240)
    if df.index[-1] > N:
        count5ma = 0 # Close&Open,Close
        df = df[(len(df)-N-1):]
        df.reset_index(inplace=True,drop=True)
        for ts in range(N):
            Failcount = []
            Failitem = []
            top = max([df['Close'][1],df['Open'][1]])
            if top >= df['P_5'][1] and top >= df['Zero'][1]:
                count5ma += 1
            else:
                count5ma = 0

            Failcount.append(df['Value'][1])
            Failcount.append(df['Value'][0])
            Failcount.append((df['High'][1] - df['Close'][1])*100.0/df['Zero'][1])
            Failcount.append((df['Close'][1] - df['Open'][1])*100.0/df['Zero'][1] ) 
            Failcount.append((df['Close'][1]- df['Low'][1])*100.0/df['Zero'][1] )
            if df['Close'][1]- df['Low'][1] == 0:
                Failcount.append(100)
            else:
                Failcount.append((df['High'][1] - df['Close'][1])*100.0/(df['Close'][1]- df['Low'][1]))
            if count5ma >= 3:
                Failitem = []
            else:
                Failitem = ['Close']
            if df['Value'][1]< 4e6:
                Failitem.append('Value1')
            if df['Value'][0]< 4e6:
                Failitem.append('Value0')
            MAcount = []
            if df['P_5'][0] > df['P_5'][1]:
                MAcount.append('P_5')
            if df['P_10'][0] > df['P_10'][1]:
                MAcount.append('P_10')
            if df['P_20'][0] > df['P_20'][1]:
                MAcount.append('P_20')
            if df['P_60'][0] > df['P_60'][1]:
                MAcount.append('P_60')
            if df['P_120'][0] > df['P_120'][1]:
                MAcount.append('P_120')
            if df['P_240'][0] > df['P_240'][1]:
                MAcount.append('P_240')
            if len(MAcount) > 1 :
                Failitem.append(','.join(s for s in MAcount))
            if df["D_K_9"][0] > df["D_K_9"][1]*1.01:
                Failitem.append('K')
            if df["D_DIFF_12_26"][0] > df["D_DIFF_12_26"][1]*1.01 or df["D_DIFF_12_26"][1] < 0:
                Failitem.append('D_DIFF_12_26')
            if df["VDIFF"][0] > df["VDIFF"][1]*1.01 or df["VDIFF"][1] < 0:
                Failitem.append('VDIFF')
            if (df["High"][1] - df["Close"][1])*2.0 > (df["Close"][1] - min([df["Zero"][1],df["Low"][1]])):
                if 100.0*(df["High"][1] - df["Low"][1])/df["Zero"][1] > 2: 
                    Failitem.append('TrueRed')
                
            BingoCount = sum(1 if df[x][1] > 0  else 0 for x in BingoList)   
            Result =    [tblname[3:],df['TradeDate'][1],df['Close'][1],100.0*df['Close'][1]/df['Zero'][1]-100]
            Result +=   [len(Failitem),'5MA']
            Result +=   [BingoCount,df['Value'][1],df['FI_IV_Ratio'][1],df['Bingo'][1]]
            Result +=   Failcount
            ResultList.append(Result)
            df.drop(0, inplace=True)
            df.reset_index(inplace=True,drop=True)
    return ResultList
def CheckBingoCriteria(tblname, N = 1):
    ResultList = []
    BingoList = ['MF1day','MF5day','MF10day','MF20day','MF60day','FI','FI10day','IV','IV10day']
    BiasDiff = ["D_BiasDiff_5_10","D_BiasDiff_5_20","D_BiasDiff_5_60","D_BiasDiff_10_20","D_BiasDiff_10_60","D_BiasDiff_20_60"]
    sqlString ="""
                    select *
                    from [tblname]
                    order by TradeDate desc
                    limit [Count]
                """.replace('[Count]',str(N+500))
    df = pd.read_sql_query(sqlString.replace('[tblname]',tblname), conn)
    df = df.sort_values(by='TradeDate', ascending=True)
    df.reset_index(inplace=True,drop=True)
    df['D_MoM_9'] = TAI.MOM(df, 9)
    df = TAI.VMACD(df, 12, 26)
    df['VOSC_Gain'] = df["VOSC"] - df["VOSC"].rolling(3).min()
##    df = TAI.PSAR(df)
##    df['D_SAR_Close'] = pd.eval('100*(df.PSAR - df.Close)/df.Zero')
    df = df.sort_values(by='TradeDate', ascending=False)
    df.reset_index(inplace=True,drop=True)
    if df.index[-1] > N:
        for ts in range(N):
            Failcount = []
            Failitem = []         
            if not (3 < df['TotalCapital'][0] < 500)                                                                : Failitem.append(0)
            if df['D_Pressure_120_5'][0] > 150                                                                      : Failitem.append(1)
            if df['W_Pressure_96_10'][0] > 140                                                                      : Failitem.append(2)
            if sum(1 if df[x][0] < 5  else 0 for x in BiasDiff) < 3                                                 : Failitem.append(3)
            if sum(1 if df[x][0] < 20 else 0 for x in BiasDiff) < 6                                                 : Failitem.append(4)
            if sum(1 if df[x][0] > 20 else 0 for x in ["D_Bias_5","D_Bias_10","D_Bias_20","D_Bias_60"]) > 0         : Failitem.append(5)
            if sum(1 if df[x][0] > 50 else 0 for x in ["W_Bias_4","W_Bias_13","W_Bias_26","W_Bias_52"]) > 0         : Failitem.append(6)
            if not (1 < df["D_K_9"][0] - df["D_D_9"][0] < 12.2)                                                     : Failitem.append(7) 
            if df["D_K_9"][0] - df["D_K_9"][1] < 2                                                                  : Failitem.append(8)
            if df["D_K_9"][0] < 55                                                                                  : Failitem.append(9)
            if df["D_NK_9"][0] - df["D_ND_9"][0] < -5                                                               : Failitem.append(10)
            if df["D_K_9"][0] - df["D_K_9"][1] - (df["D_NK_9"][0] - df["D_K_9"][0]) < 0                             : Failitem.append(11)
            if df["D_NK_9"][0] < 60                                                                                 : Failitem.append(12)
            if df["D_OSC"][0] - df["D_OSC"][1] < 0.3                                                                : Failitem.append(13)
            if not ( -1.5 <= df["D_OSC"][1] <= 1.25)                                                                : Failitem.append(14)
            if df["D_OSC"][0] > 3                                                                                   : Failitem.append(15)
            if not ( 0.1 <= df["D_DIFF_12_26"][0] <= 4.9)                                                           : Failitem.append(16)
            if df["D_NOSC"][0] - df["D_OSC"][0] < -5.2                                                              : Failitem.append(17)
            if df["D_NOSC"][0] - df["D_OSC"][0]-(df["D_OSC"][0] - df["D_OSC"][1]) > 0                               : Failitem.append(18)
##            if not (1e7 < df['Value'][0] < 4e9)                                         : Failitem.append(19)
            if  df['Value'][1] < 4e6                                                                                : Failitem.append(20)
            if sum(df['Volume'][1:6]) > 0 :
                if not (1 < df['Volume'][0] / (sum(df['Volume'][1:6])/5) <12)                                       : Failitem.append(21)
            else                                                                                                    : Failitem.append(21)
            if df['Close'][0] > 300                                                                                 : Failitem.append(23)
            if not(9.8>(df['D_MoM_9'][0]-df['D_MoM_10'][0]) > -3.6)                                                 : Failitem.append(24)
            if not (9>(df['D_MoM_10'][0] - df['D_MoM_10'][1])>0 )                                                   : Failitem.append(25)
            if (df['High'][0] - df['Close'][0])*100.0/df['Zero'][0] > 2                                             : Failitem.append(26)
            if not (-0.5 < (df['Close'][0] - df['Open'][0])*100.0/df['Zero'][0])                                    : Failitem.append(27)
            if not (2 < (df['Close'][0]- df['Low'][0])*100.0/df['Zero'][0])                                         : Failitem.append(28)
            if df['Close'][0]- df['Low'][0] == 0                                                                    : Failitem.append(29)
            else:
                if (df['High'][0] - df['Close'][0])*100.0/(df['Close'][0]- df['Low'][0]) > 50                       : Failitem.append(29)
                
            if not ( df['FI_IV_Ratio'][0] >= -50)                                                                   : Failitem.append(30)
            BingoCount = sum(1 if df[x][0] > 0  else 0 for x in BingoList)   
            if BingoCount < 4                                                                                       : Failitem.append(31)
            Failcount.append(df['TotalCapital'][0])  # <300
            Failcount.append(df['D_Pressure_120_5'][0])
            Failcount.append(df['W_Pressure_96_10'][0])
            BiasDiff = ["D_BiasDiff_5_10","D_BiasDiff_5_20","D_BiasDiff_5_60","D_BiasDiff_10_20","D_BiasDiff_10_60","D_BiasDiff_20_60"]
            Failcount.append(sum(1 if df[x][0] < 5  else 0 for x in BiasDiff))
            Failcount.append(sum(1 if df[x][0] < 20 else 0 for x in BiasDiff))
            Failcount.append(sum(1 if df[x][0] > 20 else 0 for x in ["D_Bias_5","D_Bias_10","D_Bias_20","D_Bias_60"]))
            Failcount.append(sum(1 if df[x][0] > 50 else 0 for x in ["W_Bias_4","W_Bias_13","W_Bias_26","W_Bias_52"]))
            Failcount.append(df["D_K_9"][0] - df["D_D_9"][0])
            Failcount.append(df["D_K_9"][0] - df["D_K_9"][1])
            Failcount.append(df["D_K_9"][0])
            Failcount.append(df["D_NK_9"][0] - df["D_ND_9"][0])
            Failcount.append(df["D_K_9"][0] - df["D_K_9"][1] - (df["D_NK_9"][0] - df["D_K_9"][0])) #
            Failcount.append(df["D_NK_9"][0])
            Failcount.append(df["D_OSC"][0] - df["D_OSC"][1] )   #
            Failcount.append(df["D_OSC"][1])
            Failcount.append(df["D_OSC"][0])
            Failcount.append(df["D_DIFF_12_26"][0])
            Failcount.append(df["D_NOSC"][0] - df["D_OSC"][0])   
##            Failcount.append(df["D_NDIFF_12_26"][0] )           #d
            Failcount.append(df["D_NOSC"][0] - df["D_OSC"][0]-(df["D_OSC"][0] - df["D_OSC"][1]))           #
            Failcount.append(df['Value'][0])
            Failcount.append(df['Value'][1])
            Failcount.append(float(df['Volume'][0]) /(sum(df['Volume'][1:6])/5)  ) # > 0.026
            Failcount.append(df['D_MoM_9'][0]-df['D_MoM_10'][0])
            Failcount.append((df['D_MoM_10'][0] - df['D_MoM_10'][1]))
            Failcount.append((df['High'][0] - df['Close'][0])*100.0/df['Zero'][0])
            Failcount.append((df['Close'][0] - df['Open'][0])*100.0/df['Zero'][0] ) 
            Failcount.append((df['Close'][0]- df['Low'][0])*100.0/df['Zero'][0] )
            if df['Close'][0]- df['Low'][0] == 0:
                Failcount.append(100)
            else:
                Failcount.append((df['High'][0] - df['Close'][0])*100.0/(df['Close'][0]- df['Low'][0]))
            Failcount.append(df["VOSC"][0])
            Failcount.append(df["VDIFF"][0])
            Failcount.append(df["VMACD"][0])
            Failcount.append(df['VOSC_Gain'][0])
            Result =    [tblname[3:],df['TradeDate'][0],df['Close'][0],100.0*df['Close'][0]/df['Zero'][0]-100]
            Result +=   [len(Failitem),','.join(str(s) for s in Failitem)]
            Result +=   [BingoCount,df['Value'][0],df['FI_IV_Ratio'][0],df['Bingo'][0]]
    
            Result +=   Failcount
            ResultList.append(Result)
            df.drop(0, inplace=True)
            df.reset_index(inplace=True,drop=True)
    return ResultList

if __name__ == '__main__':
    tblList = []
    print datetime.datetime.now(),'Connect to DB'
    conn = sqlite3.connect(DBpath, check_same_thread=False)
    c = conn.cursor()
    MethodList = ['KeyKbar','SAR','WDA']

    print datetime.datetime.now(),'Successful connect to',DBpath    
    cd = CrawlData(tblList)
    print datetime.datetime.now(),'Check Column Exist'
    for tblname in sorted(tblList):
        if len(tblname) == 7:
            CheckColumnExist(tblname,CheckColumnList)
            conn.commit()
            
    print datetime.datetime.now(),'Get Bingo Data'
    GetBingoData()
    print datetime.datetime.now(),'CalculateData'
    sc = 1
    for tblname in sorted(tblList):
        sc += 1
        if len(tblname) == 7:
            print '\r [%-20s] %d%%' % ('='*int(20*sc/len(tblList)), 100*sc/len(tblList)),datetime.datetime.now(),tblname,
            CalculateData(tblname)
    conn.commit()
    CheckSellList('Onhand.csv',MethodList,BackTest = False,DrawChart = True,outputpath = 'C:\\Users\\dhuang01\\Desktop\\Print\\tsec-master\\onhand')
    CheckSellList('homework.csv',MethodList,BackTest = False,DrawChart = True,outputpath = 'C:\\Users\\dhuang01\\Desktop\\Print\\tsec-master\\homework')
    print ''
    print datetime.datetime.now(),'Find Match'
    sc = 1
    rowList = []
    for tblname in sorted(tblList):
        sc += 1
        if len(tblname) == 7:
            print '\r [%-20s] %d%%' % ('='*int(20*sc/len(tblList)), 100*sc/len(tblList)),datetime.datetime.now(),tblname,
            rowList += CheckBingoCriteria(tblname,3)
            rowList += Above5MA(tblname,3)
        

    FileName = 'Result2.csv'
    fp = open(FileName,'wb')
    fout = csv.writer(fp)
    Header =    ['ID','TradeDate','Close','Raise']
    Header +=   ['FailCount','FailItem']
    Header +=   ['BingoCount','Value','FI_IV_Ratio','Bingo']
    Header +=   ['TotalCapital', 'D_Pressure_120_5', 'W_Pressure_96_10', 'BiasDiff_B5', 'D_BiasDiff_B20', 'D_Bias_A20', 'W_Bias_A50', 'D_KbD',
               'D_K_9_Delta', 'D_K_9', 'DN_KbD', 'DN_K_9_Delta', 'DN_K_9', 'D_OSC_Delta', 'D_OSC_1', 'D_OSC', 'D_DIFF', 'DN_OSC_Delta', 'DN_DIFF',
               'Value', 'Value1', 'Volume_Time','D_MoM_10', 'D_MoM_10_Delta', 'LH', 'RaiseB', 'TR', 'DK','VOSC','VDIFF','VMACD','VOSC_Gain']
    fout.writerow(Header)
    for row in rowList:
        fout.writerow(row)
    fp.close()
    print ''
    print datetime.datetime.now(),'Check Profit'
    MethodList = []
    MethodList = ['DongDa','SAR']
    CheckSellList(FileName,MethodList,DrawChart = True)
    print datetime.datetime.now(),'Done'
    conn.close()
        
    