# -*- coding: utf-8 -*-
import PyWinDDE
import datetime
import sys
import time


#f1 = open("data/stock_02.csv","a")
f2= open("stock_03.csv","a")


def recNextTickData(value,item):
	out_string =  ("%s"%(value)).split(';')
	print out_string
##	f2.write(out_string + "\n")

while True:
	try:
		dde = PyWinDDE.DDEClient("XQKGIAP","Quote")
		print "Sucessfully Connect to DDE server ..."
		break
	except:
		e = sys.exc_info()[0]
		print "Error: %s, try to connect 10 mins later."%e
		time.sleep(600)
		print "Connect to DDE server again..."

print "Connected to DDE server, start listening..."
dde.advise("2443.TW-ID,TradingDate,Time,Price,Volume,TotalVolume,High,Low,Open,Value",callback = recNextTickData)
PyWinDDE.WinMSGLoop()

