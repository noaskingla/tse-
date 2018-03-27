import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.finance import candlestick_ohlc
import TechnicalAnalysisIndicators as TAI
import numpy as np
import os
def DongDa(df,TradeList,BackTest = True): 
    SellList = []
    for i in range(len(df)):
        if df['TradeDate'][i] in TradeList:
            TradeList.remove(df['TradeDate'][i])
            bardf = df.drop(df.index[0:i-1])
            bardf['Volume'] = df['Volume']/1000
            bardf['V_5ma'] = df['Volume'].rolling(window=5).mean()[i-1:]/1000
            bardf['V_10ma'] = df['Volume'].rolling(window=10).mean()[i-1:]/1000
            bardf['V_20ma'] = df['Volume'].rolling(window=20).mean()[i-1:]/1000
            bardf.reset_index(inplace=True,drop=True)
            length = len(bardf)
            bardf['MS'] = pd.Series([None] * length)
            bardf['SellPoint'] = pd.Series([None] * length)
            MS = bardf['Close'][0] *0.9
            MaxClose = bardf['Close'][0]
            BuyPrice = bardf['Close'][0]
            SellInfo = ['Keep',bardf['TradeDate'][0],bardf['Close'][0],MS,[df['TradeDate'][i]]]
            for j in range(1,len(bardf)):
                if bardf['TradeDate'][j] in TradeList:
                    if bardf['TradeDate'][j] not in SellInfo[-1]:
                        SellInfo[-1].append(bardf['TradeDate'][j])
                    TradeList.remove(bardf['TradeDate'][j])
                    BuyPrice = bardf['Close'][j]

                if bardf['Close'][j] > MaxClose: MaxClose = bardf['Close'][j]
                if MaxClose >= 1.6 * BuyPrice:
                    if bardf['High'][j] *0.85 > MS:
                        MS = bardf['High'][j] *0.85
                if MaxClose >= 1.2 * BuyPrice:
                    if bardf['High'][j] *0.9 > MS:
                        MS = bardf['High'][j] *0.9
                elif MaxClose >= 1.1 * BuyPrice:
                    if bardf['High'][j] *0.9 > MS:
                        MS = bardf['High'][j] *0.93
                else:
                    if bardf['High'][j] *0.9 > MS:
                        MS = bardf['High'][j] *0.95
                bardf.loc[j,'MS']  = MS
                #SellInfo = ['Keep',bardf['TradeDate'][0],bardf['Close'][0],MS]
                SellInfo[1] = bardf['TradeDate'][j]
                SellInfo[2] = bardf['Close'][j]
                SellInfo[3] = MS
                if bardf['Close'][j] < MS:
                    bardf.loc[j,'SellPoint']  = bardf['Close'][j]
                    SellInfo[0] = 'Sell'
                    if BackTest == True:
                        break
                else:
                    SellInfo[0] = 'Keep'
            SellList.append(SellInfo)
            if not TradeList:
                break        
    return SellList

def DongDa5(df,Stock,n,BasicBuy=100000,BackTest = True): 
    [TradeDate,BuyPrice] = Stock
    MS = BuyPrice*0.95
    Result = ['Keep',0,0,0,0,0] # Comment,SellDate,SellPrice,MS,MSCount,estGain
    HighClose = BuyPrice
    BuyCount = int(BasicBuy / BuyPrice/1000) + (BasicBuy/1000/BuyPrice % 5 > 0)
    BuyAmount = BuyCount * BuyPrice * 1000 * 1.001425
    for i in range(len(df)):
        if df['TradeDate'][i] >= TradeDate:
            if df['Close'][i] > HighClose: HighClose = df['Close'][i]
            if HighClose >= 1.6 * BuyPrice:
                if df['High'][i] *0.85 > MS:
                    MS = df['High'][i] *0.85
            if HighClose >= 1.2 * BuyPrice:
                if df['High'][i] *0.9 > MS:
                    MS = df['High'][i] *0.9
            elif HighClose >= 1.1 * BuyPrice:
                if df['High'][i] *0.9 > MS:
                    MS = df['High'][i] *0.93
            else:
                if df['High'][i] *0.9 > MS:
                    MS = df['High'][i] *0.95
            Result[1] = str(df['TradeDate'][i])[:4]+'-'+str(df['TradeDate'][i])[4:6] + '-' + str(df['TradeDate'][i])[-2:]
            Result[2] = df['Close'][i]
            Result[3] = MS
            SellAmount = BuyCount * MS* 1000 * (1-0.001425-0.003)
            Result[5] = SellAmount - BuyAmount            
            if df['Close'][i] <  MS and df['Close'][i] < df['MA_'+str(n)][i]:
                Result[0] = 'Sell'
                Result[4] +=1 
                if BackTest: break
            else:
                Result[0] = 'Keep'
    return Result

def MA(df,Stock,n=5,BasicBuy=100000,BackTest = True): 
    [TradeDate,BuyPrice] = Stock
    Result = ['Keep',0,0,0,0,0] # Comment,SellDate,SellPrice,MS,MSCount,estGain
    HighClose = BuyPrice
    BuyCount = int(BasicBuy / BuyPrice/1000) + (BasicBuy/1000/BuyPrice % 5 > 0)
    BuyAmount = BuyCount * BuyPrice * 1000 * 1.001425
    for i in range(len(df)):
        if df['TradeDate'][i] >= TradeDate:
            Result[1] = str(df['TradeDate'][i])[:4]+'-'+str(df['TradeDate'][i])[4:6] + '-' + str(df['TradeDate'][i])[-2:]
            Result[2] = df['Close'][i]
            Result[3] = df['MA_'+str(n)][i]
            SellAmount = BuyCount * Result[3] * 1000 * (1-0.001425-0.003)
            Result[5] = SellAmount - BuyAmount
            if df['Close'][i] < df['MA_'+str(n)][i]:
                Result[0] = 'Sell'
                Result[4] +=1 
                if BackTest: break
            else:
                Result[0] = 'Keep'
    return Result

def WDa(df,Stock,BasicBuy=100000,BackTest = True): 
    [TradeDate,BuyPrice] = Stock
    MS = BuyPrice*0.9
    Result = ['Keep',0,0,0,0,0] # Comment,SellDate,SellPrice,MS,MSCount,estGain
    HighClose = BuyPrice
    BuyCount = int(BasicBuy / BuyPrice/1000) + (BasicBuy/1000/BuyPrice % 5 > 0)
    BuyAmount = BuyCount * BuyPrice * 1000 * 1.001425
    for i in range(len(df)):
        if df['TradeDate'][i] >= TradeDate:
            if df['Close'][i] > HighClose: HighClose = df['Close'][i]
            if HighClose >= 1.6 * BuyPrice:
                if df['High'][i] *0.85 > MS:
                    MS = df['High'][i] *0.85
            else:
                if df['High'][i] *0.9 > MS:
                    MS = df['High'][i] *0.9
            Result[1] = str(df['TradeDate'][i])[:4]+'-'+str(df['TradeDate'][i])[4:6] + '-' + str(df['TradeDate'][i])[-2:]
            Result[2] = df['Close'][i]
            Result[3] = MS
            SellAmount = BuyCount * Result[3] * 1000 * (1-0.001425-0.003)
            Result[5] = SellAmount - BuyAmount
            if df['Close'][i] <  MS:
                Result[0] = 'Sell'
                Result[4] +=1 
                if BackTest: break
            else:
                Result[0] = 'Keep'
    return Result

def SAR(df,TradeList, iaf = 0.02, maxaf = 0.2,BackTest = True):
    # Starting values
    SellList = []
    ContinuousCount = 0
    for i in range(len(df)):   
        if df['TradeDate'][i] in TradeList:
            TradeList.remove(df['TradeDate'][i])
            bardf = df.drop(df.index[0:i-1])
            bardf.reset_index(inplace=True,drop=True)
            length = len(bardf)
            bardf['psar'] = pd.eval('bardf.Close')
            bardf['MS'] = pd.Series([None] * length)
            bull = True
            af = iaf
            ep = bardf['Close'][0]
            hp = bardf['High'][0]
            lp = bardf['Low'][0]
            cc = 0
            MS = bardf['Close'][0] *0.9
            SellInfo = ['Keep',bardf['TradeDate'][0],bardf['Close'][0],MS,[df['TradeDate'][i]]]
            for j in range(2,length):
                if bardf['TradeDate'][j] in TradeList:
                    if bardf['TradeDate'][j] not in SellInfo[-1]:
                        SellInfo[-1].append(bardf['TradeDate'][j])
                    TradeList.remove(bardf['TradeDate'][j])
                    af = iaf
                    ep = bardf['Close'][j]
                    hp = bardf['High'][j]
                    lp = bardf['Low'][j]
                bardf.loc[j,'psar']  = bardf['psar'][j - 1] + af * (hp - bardf['psar'][j - 1])
                if bardf['High'][j] > hp:
                    hp = bardf['High'][j]
                    af = min(af + iaf, maxaf)
                if bardf['Low'][j - 1] < bardf['psar'][j]:
                     bardf.loc[j,'psar']  = bardf['Low'][j - 1]
                if bardf['Low'][j - 2] < bardf['psar'][j]:
                     bardf.loc[j,'psar']  = bardf['Low'][j - 2] 
                if bardf['psar'][j] > MS:
                    MS = bardf['psar'][j]
                if bardf['Close'][j] < MS:
                    cc +=1
                else:
                    cc = 0
                if bardf['psar'][j]> MS: MS =  bardf['psar'][j]
                bardf.loc[j,'MS']  = MS
                SellInfo[1] = bardf['TradeDate'][j]
                SellInfo[2] = bardf['Close'][j]
                SellInfo[3] = MS
                if cc >= 2:
                    SellInfo[0] = 'Sell'
                    if BackTest == True:
                        break
                else:
                    SellInfo[0] = 'Keep'
            SellList.append(SellInfo)
            if not TradeList:
                break       
            
    return SellList

def KeyKbar(df,TradeList,BackTest = True):
    # Starting values
    SellList = []
    for i in range(len(df)):
        if df['TradeDate'][i] in TradeList:
            TradeList.remove(df['TradeDate'][i])
            bardf = df.drop(df.index[0:i-1])
            bardf['Volume'] = df['Volume']/1000
            bardf['V_5ma'] = df['Volume'].rolling(window=5).mean()[i-1:]/1000
            bardf['V_10ma'] = df['Volume'].rolling(window=10).mean()[i-1:]/1000
            bardf['V_20ma'] = df['Volume'].rolling(window=20).mean()[i-1:]/1000
            bardf.reset_index(inplace=True,drop=True)
            length = len(bardf)
            bardf['KeyKbar'] = pd.Series([None] * length)
            bardf['RaiseP'] = pd.eval('100.0*(bardf.Close - bardf.Zero)/bardf.Zero')
            bardf['JumpP'] = pd.eval('100.0*(bardf.Open - bardf.Zero)/bardf.Zero')
            bardf['TrueLow'] = bardf['Low'].combine(bardf['Zero'], min, 0)
            bardf['MS'] = pd.Series([None] * length)
            bardf['SellPoint'] = pd.Series([None] * length)
            KeyBarValue = bardf['Zero'][0]
            MS = bardf['Close'][0] *0.9
            MaxClose = bardf['High'][0]
            SellInfo = ['Keep',bardf['TradeDate'][0],bardf['Close'][0],MS,[df['TradeDate'][i]]]
            for j in range(1,len(bardf)):
                if bardf['TradeDate'][j] in TradeList:
                    if bardf['TradeDate'][j] not in SellInfo[-1]:
                        SellInfo[-1].append(bardf['TradeDate'][j])
                    TradeList.remove(bardf['TradeDate'][j])
                Vcount = 0
                if bardf['Volume'][j] > bardf['V_5ma'][j]: Vcount += 1
                if bardf['Volume'][j] > bardf['V_10ma'][j]: Vcount += 1
                if bardf['Volume'][j] > bardf['V_20ma'][j]: Vcount += 1
                if Vcount == 3 and bardf['RaiseP'][j] > 0 :
                    if bardf['TrueLow'][j] > KeyBarValue and bardf['Close'][j]  > MaxClose:
                        MaxClose = bardf['High'][j]
                        KeyBarValue = bardf['TrueLow'][j]
                if Vcount == 2 and bardf['JumpP'][j] > 2 :
                    if bardf['TrueLow'][j] > KeyBarValue and bardf['Close'][j]  > MaxClose:
                        MaxClose = bardf['High'][j]
                        KeyBarValue = bardf['TrueLow'][j]
                if bardf['RaiseP'][j] > 3:
                    if bardf['TrueLow'][j] > KeyBarValue and bardf['Close'][j]  > MaxClose:
                        MaxClose = bardf['High'][j]
                        KeyBarValue = bardf['TrueLow'][j]
                bardf.loc[j,'KeyKbar']  = KeyBarValue
                if bardf['TrueLow'][j] <   KeyBarValue:
                    if bardf['TrueLow'][j] > MS:
                        MS = bardf['TrueLow'][j]
                bardf.loc[j,'MS']  = MS
                #SellInfo = ['Keep',bardf['TradeDate'][0],bardf['Close'][0],MS]
                SellInfo[1] = bardf['TradeDate'][j]
                SellInfo[2] = bardf['Close'][j]
                SellInfo[3] = MS
                if bardf['Close'][j] < MS:
                    bardf.loc[j,'SellPoint']  = bardf['Close'][j]
                    SellInfo[0] = 'Sell'
                    if BackTest == True:
                        break
                else:
                    SellInfo[0] = 'Keep'
            SellList.append(SellInfo)
            if not TradeList:
                break        
    return SellList

def Resistance_Support(df):
    Top = df['Close'].combine(df['Open'], max, 0).rolling(window=5,center=True).max()
    Btm = df['Close'].combine(df['Open'], min, 0).rolling(window=5,center=True).min()
    MA_5 = df['Close'].rolling(window=5,center=True).mean()
    MA_5Max = MA_5.rolling(window=5,center=True).max()
    MA_5Min = MA_5.rolling(window=5,center=True).min()
    Support = []
    SupportX = []
    for x in range(len(df)):
        if MA_5[x] == MA_5Min[x]:
            Support.append(Btm[x])
            SupportX.append(x)
        if MA_5[x] == MA_5Max[x]:
            Support.append(Top[x])
            SupportX.append(x)
        x+=1
    data = [[0]]
    dataX = [[0]]
    for rs,x in sorted(zip(Support,SupportX)):
        if (rs-np.median(data[-1]))/rs < 0.01:
            data[-1].append(rs)
            dataX[-1].append(x)
        else:
            data.append([rs])
            dataX.append([x])
    Support = [np.median(rs) for rs in data[1:]]
    SupportX = [np.min(x) for x in dataX[1:]]
    return [SupportX,Support]

def TurnJoint(dfc,ws = 5,SL = []):
    MA_5Max = dfc.rolling(window=ws,center=True).max()
    MA_5Min = dfc.rolling(window=ws,center=True).min()
    Support = [[],[]]
    Resistance = [[],[]]
    for x in range(len(dfc)):
        if dfc[x] == MA_5Min[x]:
            if SL:
                if SL[1] > dfc[x] > SL[0] :
                    Support[1].append(dfc[x])
                    Support[0].append(x)
            else:
                Support[1].append(dfc[x])
                Support[0].append(x)
        if dfc[x] == MA_5Max[x]:
            if SL:
                if SL[1] > dfc[x] > SL[0] :
                    Resistance[1].append(dfc[x])
                    Resistance[0].append(x)
            else:
                Resistance[1].append(dfc[x])
                Resistance[0].append(x)
        x+=1
    Resistance[0].append(x)
    Support[0].append(x)
##    data = [[0]]
##    dataX = [[0]]
##    for rs,x in sorted(zip(Support,SupportX)):
##        if (rs-np.median(data[-1]))/rs < 0.01:
##            data[-1].append(rs)
##            dataX[-1].append(x)
##        else:
##            data.append([rs])
##            dataX.append([x])
##    Support = [np.median(rs) for rs in data[1:]]
##    SupportX = [np.min(x) for x in dataX[1:]]
    return [Support,Resistance]   

def DrawChart(df,tblname,StockHis,outputpath,ChartLen = 240):
    fig = plt.figure(figsize=(12, 6))
    ax1 = plt.subplot2grid((6,3), (0, 0), colspan=3,rowspan=3)
    sellType = {'DongDa':'k*','WDa':'b*','SAR':'r*','KeyKbar':'g*'}
    ohlc = []
    SellList = {}
    BuyList = []
    dropLen = len(df)
    RS = Resistance_Support(df)
    df['P_60'] = TAI.MA(df['Close'],60)
    df['P_120'] = TAI.MA(df['Close'],120)
    df['P_240'] = TAI.MA(df['Close'],240)
    df['P_20'] = TAI.MA(df['Close'],20)
    df['P_10'] = TAI.MA(df['Close'],10)
    df['P_5'] = TAI.MA(df['Close'],5)
    df['V_5'] = TAI.MA(df['Volume']/1000,5)
    df['V_10'] = TAI.MA(df['Volume']/1000,10)
    df['V_20'] = TAI.MA(df['Volume']/1000,20)
    df = TAI.VMACD(df, 12, 26, nsig=9)
    if dropLen > ChartLen:
        dropLen -= ChartLen
        df = df.drop(df.index[0:dropLen])
        df.reset_index(inplace=True,drop=True)
    x = 0
    y = len(df)
    
    while x < y:
        append_me = x, df['Open'][x], df['High'][x], df['Low'][x], df['Close'][x], df['Volume'][x]
        ohlc.append(append_me)
        if df['TradeDate'][x] in StockHis['Buy']:
            BuyList.append([x,df['Low'][x]*0.99])
        if df['TradeDate'][x] in StockHis['Sell']:
            modepercent = 0.99
            for method in StockHis['Sell'][df['TradeDate'][x]]:
                if method not in SellList:
                    SellList[method] = []
                SellList[method].append([x,df['Low'][x]*modepercent])
                modepercent -= 0.01
        x+=1
    limitY = [np.nanmin(df['P_5'].values),np.nanmax(df['Close'].values)]
    
    BuyList = zip(*BuyList)
    
    for rs in zip(*RS):
        x = rs[0] - dropLen
        if rs[1] >= df['Low'][y-1]:
            ax1.plot([x,y], [rs[1],rs[1]],'r-',linewidth=0.8)
        else:
            ax1.plot([x,y], [rs[1],rs[1]],'b-',linewidth=0.5)
        
    candlestick_ohlc(ax1, ohlc, width=0.4, colorup='#db3f3f', colordown='#77d879')
    X = [i for i in range(y)]
    ax1.plot(X, df['P_60'],'c-',linewidth=1)
    ax1.plot(X, df['P_120'], 'm-',linewidth=1)
    ax1.plot(X, df['P_240'], 'y-',linewidth=1)
    ax1.plot(X, df['P_20'], 'k-',linewidth=1)
    ax1.plot(X, df['P_10'], 'k-',linewidth=1)
    ax1.plot(X, df['P_5'], 'k-',linewidth=1)
    MSP = [[],[]]
    for i in [5,10,20,60,120]:
        try:
            MSP[1].append(df['Close'][y-i])
            MSP[0].append(y-i)
        except:
            print y,i
    
    ax1.plot(MSP[0],MSP[1],"k.")
    if BuyList:
        ax1.plot(BuyList[0],BuyList[1],'b*')
    if SellList:
        for method in SellList:
            data = zip(*SellList[method])
            ax1.plot(data[0],data[1],sellType[method])
    
    ax1.grid(True)
    pad = 0.25
    ax1.set_ylim(limitY[0]-(limitY[1]-limitY[0])*pad,limitY[1]*1.05)
    ax2 = ax1.twinx()    
    ax2.bar(X,df['Volume']/1000,color='green',width=1,align='center', linewidth=1)
    ax2.plot(X, df['V_5'],'b-',linewidth=1)
    ax2.plot(X, df['V_10'],'m-',linewidth=1)
    ax2.plot(X, df['V_20'],'y-',linewidth=1)
    plt.title(tblname, loc='left')
    yticks = ax2.get_yticks()
    ax2.set_yticks(yticks[::3])
    ax2.yaxis.set_label_position("right")
    ax2.set_ylim(0,ax2.get_ylim()[1]*4)
    ax3 = plt.subplot2grid((6,3), (3, 0), colspan=3,sharex=ax1)
##    ax3.bar(X,df['D_K_9']-df['D_D_9'],color='k',width=1,align='center', linewidth=1)
##    ax4 = ax3.twinx()
    ax3.plot(X, df['D_K_9'],'b-',linewidth=2)
    ylimit= max([abs(ax3.get_ylim()[0]),abs(ax3.get_ylim()[1])])
    ax3.set_ylim(-ylimit,ylimit)
    [Support,Resistance] = TurnJoint(df['D_K_9'],7,[0,95])
    for i in range(1,len(Support[1])):
        if Support[1][i] > Support[1][i-1]:
            y = (Support[1][i]-Support[1][i-1])/(Support[0][i]-Support[0][i-1])*(Support[0][i+1]-Support[0][i-1])+Support[1][i-1]
            ax3.plot([Support[0][i-1],Support[0][i+1]], [Support[1][i-1],y],'r-',linewidth=1)
            ax3.plot([Support[0][i-1],Support[0][i]], [Support[1][i-1],Support[1][i]],'r.',linewidth=1)
    for i in range(1,len(Resistance[1])):
        if Resistance[1][i] < Resistance[1][i-1]:
            y = (Resistance[1][i]-Resistance[1][i-1])/(Resistance[0][i]-Resistance[0][i-1])*(Resistance[0][i+1]-Resistance[0][i-1])+Resistance[1][i-1]
            ax3.plot([Resistance[0][i-1],Resistance[0][i+1]], [Resistance[1][i-1],y],'g-',linewidth=1)
            ax3.plot([Resistance[0][i-1],Resistance[0][i]], [Resistance[1][i-1],Resistance[1][i]],'g.',linewidth=1)
##    ax4.plot(X, TAI.MA(df['D_K_9'], 5).diff(5),'m-',linewidth=1)

    plt.ylabel('P KD', fontsize=8)
    ax3.set_ylim(0,100)
    
    ax5 = plt.subplot2grid((6,3), (4, 0), colspan=3,sharex=ax1)
    ax5.bar(X,df["D_DIFF_12_26"]-df["D_MACD_9"],color='k',width=1,align='center', linewidth=1)

    ax5.plot(X, df["D_DIFF_12_26"],'b-',linewidth=2)
    [Support,Resistance] = TurnJoint(df["D_DIFF_12_26"],5)
    ax5.set_ylim(ax5.get_ylim()[0],ax5.get_ylim()[1])
    for i in range(1,len(Support[1])):
        if Support[1][i] > Support[1][i-1]:
            y = (Support[1][i]-Support[1][i-1])/(Support[0][i]-Support[0][i-1])*(Support[0][i+1]-Support[0][i-1])+Support[1][i-1]
            ax5.plot([Support[0][i-1],Support[0][i+1]], [Support[1][i-1],y],'r-',linewidth=1)
            ax5.plot([Support[0][i-1],Support[0][i]], [Support[1][i-1],Support[1][i]],'r.',linewidth=1)
    for i in range(1,len(Resistance[1])):
        if Resistance[1][i] < Resistance[1][i-1]:
            y = (Resistance[1][i]-Resistance[1][i-1])/(Resistance[0][i]-Resistance[0][i-1])*(Resistance[0][i+1]-Resistance[0][i-1])+Resistance[1][i-1]
            ax5.plot([Resistance[0][i-1],Resistance[0][i+1]], [Resistance[1][i-1],y],'g-',linewidth=1)
            ax5.plot([Resistance[0][i-1],Resistance[0][i]], [Resistance[1][i-1],Resistance[1][i]],'g.',linewidth=1)
    plt.ylabel('P MACD', fontsize=8)
    
    ax7 = plt.subplot2grid((6,3), (5, 0), colspan=3,sharex=ax1)
    ax7.bar(X,df['VOSC'],color='k',width=1,align='center', linewidth=1)
    ax7.plot(X, df['VDIFF'],'b-',linewidth=2)
##    ylimit= max([abs(ax7.get_ylim()[0]),abs(ax7.get_ylim()[1])])
    ax7.set_ylim(ax7.get_ylim()[0],ax7.get_ylim()[1])
    [Support,Resistance] = TurnJoint(df['VDIFF'],5)
    for i in range(1,len(Support[1])):
        if Support[1][i] > Support[1][i-1]:
            y = (Support[1][i]-Support[1][i-1])/(Support[0][i]-Support[0][i-1])*(Support[0][i+1]-Support[0][i-1])+Support[1][i-1]
            ax7.plot([Support[0][i-1],Support[0][i+1]], [Support[1][i-1],y],'r-',linewidth=1)
            ax7.plot([Support[0][i-1],Support[0][i]], [Support[1][i-1],Support[1][i]],'r.',linewidth=1)
    for i in range(1,len(Resistance[1])):
        if Resistance[1][i] < Resistance[1][i-1]:
            y = (Resistance[1][i]-Resistance[1][i-1])/(Resistance[0][i]-Resistance[0][i-1])*(Resistance[0][i+1]-Resistance[0][i-1])+Resistance[1][i-1]
            ax7.plot([Resistance[0][i-1],Resistance[0][i+1]], [Resistance[1][i-1],y],'g-',linewidth=1)
            ax7.plot([Resistance[0][i-1],Resistance[0][i]], [Resistance[1][i-1],Resistance[1][i]],'g.',linewidth=1)

    plt.ylabel('V MACD', fontsize=8)

    
    ax1.set_xlim(X[0],X[-1]+2)
    plt.setp(ax1.get_xticklabels(), visible=False)
    plt.setp(ax3.get_xticklabels(), visible=False)
    plt.setp(ax5.get_xticklabels(), visible=False)
    
    plt.tight_layout()
    plt.savefig(os.path.join(outputpath,tblname + '.jpg'))
    plt.close()