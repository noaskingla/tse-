# -*- coding: utf-8 -*-
""" NoAskingLa 2017-08-26 """
import pandas as pd
#Moving Average
def MA(df, n):
    return df.rolling(window=n).mean()

#Exponential Moving Average

def EMA(df, n):  
    EMA = pd.Series(pd.ewma(df['Close'], span = n, min_periods = n - 1), name = 'EMA_' + str(n))  
    df = df.join(EMA)  
    return df

#Momentum

def MOM(df, n):  
    return 100.0*df['Close'].diff(n)/df['Zero']

#Rate of Change

def ROC(df, n):  
    return 100.0*df['Close'].diff(n - 1)/df['Close'].shift(n - 1) 

#Average True Range

def ATR(df, n):  
    i = 0  
    TR_l = [0]  
    while i < df.index[-1]:  
        TR = max(df.get_value(i + 1, 'High'), df.get_value(i, 'Close')) - min(df.get_value(i + 1, 'Low'), df.get_value(i, 'Close'))  
        TR_l.append(TR)  
        i = i + 1  
    TR_s = pd.Series(TR_l)  
    ATR = pd.Series(pd.ewma(TR_s, span = n, min_periods = n), name = 'ATR_' + str(n))  
    df = df.join(ATR)  
    return df

#Bollinger Bands

def BBANDS(df, n):  
    MA = pd.rolling_mean(df['Close'], n)
    MSD = pd.rolling_std(df['Close'], n)  
    df['BollingerB_' + str(n)] = pd.eval('4 * MSD / MA')   
    df['Bollinger%b_' + str(n)] = pd.eval('(df.Close - MA + 2 * MSD) / (4 * MSD) ')
    return df

#Pivot Points, Supports and Resistances

def PPSR(df):  
    df['PP'] = pd.eval('(df.High + df.Low + df.Close) / 3')  
    df['R1'] = pd.eval('2 * df.PP - df.Low')  
    df['S1'] = pd.eval('2 * df.PP - df.High')  
    df['R2'] = pd.eval('df.PP + df.High - df.Low')  
    df['S2'] = pd.eval('df.PP - df.High + df.Low')  
    df['R3'] = pd.eval('df.High + 2 * (df.PP - df.Low)')  
    df['S3'] = pd.eval('df.Low - 2 * (df.High - df.PP)')  
    return df

#Stochastic oscillator %K
def STOK(df):  
    return pd.eval('(df.Close - df.Low) / (df.High - df.Low)') 

#Stochastic oscillator %D

def STO(df, n):  
    SOk = pd.Series((df['Close'] - df['Low']) / (df['High'] - df['Low']), name = 'SO%k')  
    SOd = pd.Series(pd.ewma(SOk, span = n, min_periods = n - 1), name = 'SO%d_' + str(n))  
    df = df.join(SOd)  
##    df = df.join(SOk)  
    return df

# KD
def KD(df,n):
    def GetKD(column):
        param0, param1 = 1.0/3.0, 2.0/3.0
        k = 50.0
        for i in param1 * column:
            k = param0 * k + i
            yield k
    def GetRSV(df):
        low_min = df['Low'].rolling(min_periods=1, window=n, center=False).min()
        high_max = df['High'].rolling(min_periods=1, window=n, center=False).max()
        cv = pd.eval("(df.Close -low_min) / (high_max - low_min)")
        df[rsv_column] = cv.fillna(0).astype('float64') * 100
    rsv_column = 'RSV_'+str(n)
    k_column =  'K_'+str(n)
    d_column =  'D_'+str(n)
    GetRSV(df)
    df[k_column] = list(GetKD(df.get(rsv_column)))
    df[d_column] = list(GetKD(df.get(k_column)))
    return df
        
#Trix

def TRIX(df, n):  
    EX1 = pd.ewma(df['Close'], span = n, min_periods = n - 1)  
    EX2 = pd.ewma(EX1, span = n, min_periods = n - 1)  
    EX3 = pd.ewma(EX2, span = n, min_periods = n - 1)  
    i = 0  
    ROC_l = [0]  
    while i + 1 <= df.index[-1]:  
        ROC = (EX3[i + 1] - EX3[i]) / EX3[i]  
        ROC_l.append(ROC)  
        i = i + 1  
    Trix = pd.Series(ROC_l, name = 'Trix_' + str(n))  
    df = df.join(Trix)  
    return df

#Average Directional Movement Index  
def ADX(df, n, n_ADX):  
    i = 0  
    UpI = []  
    DoI = []  
    while i + 1 <= df.index[-1]:  
        UpMove = df.get_value(i + 1, 'High') - df.get_value(i, 'High')  
        DoMove = df.get_value(i, 'Low') - df.get_value(i + 1, 'Low')  
        if UpMove > DoMove and UpMove > 0:  
            UpD = UpMove  
        else: UpD = 0  
        UpI.append(UpD)  
        if DoMove > UpMove and DoMove > 0:  
            DoD = DoMove  
        else: DoD = 0  
        DoI.append(DoD)  
        i = i + 1  
    i = 0  
    TR_l = [0]  
    while i < df.index[-1]:  
        TR = max(df.get_value(i + 1, 'High'), df.get_value(i, 'Close')) - min(df.get_value(i + 1, 'Low'), df.get_value(i, 'Close'))  
        TR_l.append(TR)  
        i = i + 1  
    TR_s = pd.Series(TR_l)  
    ATR = pd.Series(pd.ewma(TR_s, span = n, min_periods = n))  
    UpI = pd.Series(UpI)  
    DoI = pd.Series(DoI)  
    PosDI = pd.Series(pd.ewma(UpI, span = n, min_periods = n - 1) / ATR)  
    NegDI = pd.Series(pd.ewma(DoI, span = n, min_periods = n - 1) / ATR)  
    ADX = pd.Series(pd.ewma(abs(PosDI - NegDI) / (PosDI + NegDI), span = n_ADX, min_periods = n_ADX - 1), name = 'ADX_' + str(n) + '_' + str(n_ADX))  
    df = df.join(ADX)  
    return df

#MACD, MACD Signal and MACD difference

def MACD(df, n_fast, n_slow, nsig=9, percent=True):
    Pname = ''
    EMAfast = df['Close'].ewm(ignore_na=False,span=n_fast,min_periods=n_slow - 1,adjust=True).mean()
    EMAslow = df['Close'].ewm(ignore_na=False,span=n_slow,min_periods=n_slow - 1,adjust=True).mean()
    if percent:
        Pname = ''
        df[Pname+'DIFF_' + str(n_fast) + '_' + str(n_slow)] = pd.eval("(EMAfast / EMAslow - 1.0)*100")
    else:
        Pname = 'R'
        df[Pname+'DIFF_' + str(n_fast) + '_' + str(n_slow)] = pd.eval("EMAfast - EMAslow")
    df[Pname+'MACD_9'] = df[Pname+'DIFF_' + str(n_fast) + '_' + str(n_slow)].ewm(ignore_na=False,span=9,min_periods=8,adjust=True).mean()
    df[Pname+'OSC'] = pd.eval('df.'+Pname+'DIFF_' + str(n_fast) + '_' + str(n_slow) + " - " + 'df.'+Pname+'MACD_9')
    return df

#MACD, MACD Signal and MACD difference for Volume/Value
def VMACD(df, n_fast, n_slow, nsig=9):
    EMAfast = df['Value'].ewm(ignore_na=False,span=n_fast,min_periods=n_slow - 1,adjust=True).mean()
    EMAslow = df['Value'].ewm(ignore_na=False,span=n_slow,min_periods=n_slow - 1,adjust=True).mean()
##    df['VDIFF'] = pd.eval("(EMAfast / EMAslow - 1.0)*100")
    df['VDIFF'] = pd.eval("EMAfast - EMAslow")
    df['VMACD'] = df['VDIFF'].ewm(ignore_na=False,span=9,min_periods=8,adjust=True).mean()
    df['VOSC'] = pd.eval('df.VDIFF - df.VMACD')
    return df


#Mass Index  
def MassI(df):  
    Range = df['High'] - df['Low']  
    EX1 = pd.ewma(Range, span = 9, min_periods = 8)  
    EX2 = pd.ewma(EX1, span = 9, min_periods = 8)  
    Mass = EX1 / EX2  
    MassI = pd.Series(pd.rolling_sum(Mass, 25), name = 'Mass Index')  
    df = df.join(MassI)  
    return df

#Vortex Indicator: http://www.vortexindicator.com/VFX_VORTEX.PDF  
def Vortex(df, n):  
    i = 0  
    TR = [0]  
    while i < df.index[-1]:  
        Range = max(df.get_value(i + 1, 'High'), df.get_value(i, 'Close')) - min(df.get_value(i + 1, 'Low'), df.get_value(i, 'Close'))  
        TR.append(Range)  
        i = i + 1  
    i = 0  
    VM = [0]  
    while i < df.index[-1]:  
        Range = abs(df.get_value(i + 1, 'High') - df.get_value(i, 'Low')) - abs(df.get_value(i + 1, 'Low') - df.get_value(i, 'High'))  
        VM.append(Range)  
        i = i + 1  
    VI = pd.Series(pd.rolling_sum(pd.Series(VM), n) / pd.rolling_sum(pd.Series(TR), n), name = 'Vortex_' + str(n))  
    df = df.join(VI)  
    return df





#KST Oscillator  
def KST(df, r1, r2, r3, r4, n1, n2, n3, n4):  
    M = df['Close'].diff(r1 - 1)  
    N = df['Close'].shift(r1 - 1)  
    ROC1 = M / N  
    M = df['Close'].diff(r2 - 1)  
    N = df['Close'].shift(r2 - 1)  
    ROC2 = M / N  
    M = df['Close'].diff(r3 - 1)  
    N = df['Close'].shift(r3 - 1)  
    ROC3 = M / N  
    M = df['Close'].diff(r4 - 1)  
    N = df['Close'].shift(r4 - 1)  
    ROC4 = M / N  
    KST = pd.Series(pd.rolling_sum(ROC1, n1) + pd.rolling_sum(ROC2, n2) * 2 + pd.rolling_sum(ROC3, n3) * 3 + pd.rolling_sum(ROC4, n4) * 4, name = 'KST_' + str(r1) + '_' + str(r2) + '_' + str(r3) + '_' + str(r4) + '_' + str(n1) + '_' + str(n2) + '_' + str(n3) + '_' + str(n4))  
    df = df.join(KST)  
    return df

#Relative Strength Index  
def RSI(df, n):  
    i = 0  
    UpI = [0]  
    DoI = [0]  
    while i + 1 <= df.index[-1]:  
        UpMove = df.get_value(i + 1, 'High') - df.get_value(i, 'High')  
        DoMove = df.get_value(i, 'Low') - df.get_value(i + 1, 'Low')  
        if UpMove > DoMove and UpMove > 0:  
            UpD = UpMove  
        else: UpD = 0  
        UpI.append(UpD)  
        if DoMove > UpMove and DoMove > 0:  
            DoD = DoMove  
        else: DoD = 0  
        DoI.append(DoD)  
        i = i + 1  
    UpI = pd.Series(UpI)  
    DoI = pd.Series(DoI)  
    PosDI = pd.Series(pd.ewma(UpI, span = n, min_periods = n - 1))  
    NegDI = pd.Series(pd.ewma(DoI, span = n, min_periods = n - 1))  
    RSI = pd.Series(PosDI / (PosDI + NegDI), name = 'RSI_' + str(n))  
    df = df.join(RSI)  
    return df

#True Strength Index  
def TSI(df, r, s):  
    M = pd.Series(df['Close'].diff(1))  
    aM = abs(M)  
    EMA1 = pd.Series(pd.ewma(M, span = r, min_periods = r - 1))  
    aEMA1 = pd.Series(pd.ewma(aM, span = r, min_periods = r - 1))  
    EMA2 = pd.Series(pd.ewma(EMA1, span = s, min_periods = s - 1))  
    aEMA2 = pd.Series(pd.ewma(aEMA1, span = s, min_periods = s - 1))  
    TSI = pd.Series(EMA2 / aEMA2, name = 'TSI_' + str(r) + '_' + str(s))  
    df = df.join(TSI)  
    return df

#Accumulation/Distribution

def ACCDIST(df, n):  
    ad = (2 * df['Close'] - df['High'] - df['Low']) / (df['High'] - df['Low']) * df['Volume']  
    M = ad.diff(n - 1)  
    N = ad.shift(n - 1)  
    ROC = M / N  
    AD = pd.Series(ROC, name = 'Acc/Dist_ROC_' + str(n))  
    df = df.join(AD)  
    return df

#Chaikin Oscillator

def Chaikin(df):  
    ad = (2 * df['Close'] - df['High'] - df['Low']) / (df['High'] - df['Low']) * df['Volume']  
    Chaikin = pd.Series(pd.ewma(ad, span = 3, min_periods = 2) - pd.ewma(ad, span = 10, min_periods = 9), name = 'Chaikin')  
    df = df.join(Chaikin)  
    return df

#Money Flow Index and Ratio

def MFI(df, n):  
    PP = (df['High'] + df['Low'] + df['Close']) / 3  
    i = 0  
    PosMF = [0]  
    while i < df.index[-1]:  
        if PP[i + 1] > PP[i]:  
            PosMF.append(PP[i + 1] * df.get_value(i + 1, 'Volume'))  
        else:  
            PosMF.append(0)  
        i = i + 1  
    PosMF = pd.Series(PosMF)  
    TotMF = PP * df['Volume']  
    MFR = pd.Series(PosMF / TotMF)  
    MFI = pd.Series(pd.rolling_mean(MFR, n), name = 'MFI_' + str(n))  
    df = df.join(MFI)  
    return df

#On-balance Volume

def OBV(df, n):  
    i = 0  
    OBV = [0]  
    while i < df.index[-1]:  
        if df.get_value(i + 1, 'Close') - df.get_value(i, 'Close') > 0:  
            OBV.append(df.get_value(i + 1, 'Volume'))  
        if df.get_value(i + 1, 'Close') - df.get_value(i, 'Close') == 0:  
            OBV.append(0)  
        if df.get_value(i + 1, 'Close') - df.get_value(i, 'Close') < 0:  
            OBV.append(-df.get_value(i + 1, 'Volume'))  
        i = i + 1  
    OBV = pd.Series(OBV)  
    OBV_ma = pd.Series(pd.rolling_mean(OBV, n), name = 'OBV_' + str(n))  
    df = df.join(OBV_ma)  
    return df

#Force Index

def FORCE(df, n):  
    F = pd.Series(df['Close'].diff(n) * df['Volume'].diff(n), name = 'Force_' + str(n))  
    df = df.join(F)  
    return df

#Ease of Movement

def EOM(df, n):  
    EoM = (df['High'].diff(1) + df['Low'].diff(1)) * (df['High'] - df['Low']) / (2 * df['Volume'])  
    Eom_ma = pd.Series(pd.rolling_mean(EoM, n), name = 'EoM_' + str(n))  
    df = df.join(Eom_ma)  
    return df

#Commodity Channel Index

def CCI(df, n):  
    PP = (df['High'] + df['Low'] + df['Close']) / 3  
    CCI = pd.Series((PP - pd.rolling_mean(PP, n)) / pd.rolling_std(PP, n), name = 'CCI_' + str(n))  
    df = df.join(CCI)  
    return df

#Coppock Curve

def COPP(df, n):  
    M = df['Close'].diff(int(n * 11 / 10) - 1)  
    N = df['Close'].shift(int(n * 11 / 10) - 1)  
    ROC1 = M / N  
    M = df['Close'].diff(int(n * 14 / 10) - 1)  
    N = df['Close'].shift(int(n * 14 / 10) - 1)  
    ROC2 = M / N  
    Copp = pd.Series(pd.ewma(ROC1 + ROC2, span = n, min_periods = n), name = 'Copp_' + str(n))  
    df = df.join(Copp)  
    return df

#Keltner Channel

def KELCH(df, n):  
    KelChM = pd.Series(pd.rolling_mean((df['High'] + df['Low'] + df['Close']) / 3, n), name = 'KelChM_' + str(n))  
    KelChU = pd.Series(pd.rolling_mean((4 * df['High'] - 2 * df['Low'] + df['Close']) / 3, n), name = 'KelChU_' + str(n))  
    KelChD = pd.Series(pd.rolling_mean((-2 * df['High'] + 4 * df['Low'] + df['Close']) / 3, n), name = 'KelChD_' + str(n))  
    df = df.join(KelChM)  
    df = df.join(KelChU)  
    df = df.join(KelChD)  
    return df

#Ultimate Oscillator

def ULTOSC(df):  
    i = 0  
    TR_l = [0]  
    BP_l = [0]  
    while i < df.index[-1]:  
        TR = max(df.get_value(i + 1, 'High'), df.get_value(i, 'Close')) - min(df.get_value(i + 1, 'Low'), df.get_value(i, 'Close'))  
        TR_l.append(TR)  
        BP = df.get_value(i + 1, 'Close') - min(df.get_value(i + 1, 'Low'), df.get_value(i, 'Close'))  
        BP_l.append(BP)  
        i = i + 1  
    UltO = pd.Series((4 * pd.rolling_sum(pd.Series(BP_l), 7) / pd.rolling_sum(pd.Series(TR_l), 7)) + (2 * pd.rolling_sum(pd.Series(BP_l), 14) / pd.rolling_sum(pd.Series(TR_l), 14)) + (pd.rolling_sum(pd.Series(BP_l), 28) / pd.rolling_sum(pd.Series(TR_l), 28)), name = 'Ultimate_Osc')  
    df = df.join(UltO)  
    return df

#Donchian Channel

def DONCH(df, n):  
    i = 0  
    DC_l = []  
    while i < n - 1:  
        DC_l.append(0)  
        i = i + 1  
    i = 0  
    while i + n - 1 < df.index[-1]:  
        DC = max(df['High'].ix[i:i + n - 1]) - min(df['Low'].ix[i:i + n - 1])  
        DC_l.append(DC)  
        i = i + 1  
    DonCh = pd.Series(DC_l, name = 'Donchian_' + str(n))  
    DonCh = DonCh.shift(n - 1)  
    df = df.join(DonCh)  
    return df

#Standard Deviation

def STDDEV(df, n):  
    df = df.join(pd.Series(pd.rolling_std(df['Close'], n), name = 'STD_' + str(n)))  
    return df  

# Convert Frequency from Day to Week/month/year

def ConvertFrequency(pd_ori,period_type = 'W'):
    conversion = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last','Volume':'sum','Value':'sum','Zero':'first'}
    tmp = pd_ori[['TradeDate','Close','Open','Zero','High','Low','Volume','Value']]
    tmp = tmp.set_index('TradeDate')
    tmp = tmp.resample(period_type).agg(conversion)
    tmp = tmp[tmp['Volume'].notnull()]
    tmp.reset_index(inplace=True)
    tmp = tmp.sort_values(by='TradeDate', ascending=False)
    tmp.reset_index(inplace=True)
    return  tmp

def GetBiasDiff(df,List = [5,10,20,60]):
    for i in List:
        for j in List:
            if i < j:
                df['BiasDiff_' + str(i) + '_' + str(j)] = abs(df['MA_'+str(i)] - df['MA_'+str(j)])*100 / df['Close']
    return df
def GetBias(df,MAList):
    for i in MAList:
        df['Bias_' + str(i)] = ((1.0 - df['Close']/ df['MA_'+str(i)]) )*100  
    return df
def GetPressure(df,Kbar=120,Gridpercent= 10.0,testBar= 0):
    Pdf = df[pd.notnull(df['Median'])]
    Pdf = Pdf.sort_values(by='TradeDate', ascending=False)
    Pdf.reset_index(inplace=True,drop = True)
    ZeroSlice = 0
    result = 999999
    Grid = Pdf.get_value(testBar, 'Median') * (Gridpercent/100);
    Zero = Pdf.get_value(testBar, 'Median') *((100.0-Gridpercent/2)/100);
    if Zero and Grid:
        Slices = int((max(Pdf['Median'][testBar:(Kbar+1)])- Zero)/Grid) + 1
        Data = [0 for i in range(Slices)]
        i = 0 + testBar
        result = 0
        if Slices > 1 :
            if Kbar-1 <= Pdf.index[-1]:
                while i < Kbar:
                    g = int((Pdf.get_value(i, 'Median')- Zero)/(Grid))
                    if g > 0 :
                        ZeroSlice = 1
                        Data[g] += Pdf.get_value(i, 'Value')
                    elif g == 0 :
                        if ZeroSlice == 0 :
                            Data[g] += Pdf.get_value(i, 'Value') 
                    i += 1
                result = sum(Data[1:])/Data[0] *100
    return result

def PSAR(df, iaf = 0.02, maxaf = 0.2):
    length = len(df)
    df['PSAR'] = pd.eval('df.Close')
    bull = True
    af = iaf
    ep = df['Close'][0]
    hp = df['High'][0]
    lp = df['Low'][0]
    for i in range(2,length):
        if bull:
            df.loc[i,'PSAR']  = df['PSAR'][i - 1] + af * (hp - df['PSAR'][i - 1])
        else:
            df.loc[i,'PSAR']  = df['PSAR'][i - 1] + af * (lp - df['PSAR'][i - 1])
        reverse = False
        if bull:
            if df['Close'][i] < df['PSAR'][i]:
                bull = False
                reverse = True
                df.loc[i,'PSAR']  = hp
                lp = df['Low'][i]
                af = iaf
        else:
            if df['Close'][i] > df['PSAR'][i]:
                bull = True
                reverse = True
                df.loc[i,'PSAR']  = lp
                hp = df['High'][i]
                af = iaf
    
        if not reverse:
            if bull:
                if df['High'][i] > hp:
                    hp = df['High'][i]
                    af = min(af + iaf, maxaf)
                df.loc[i,'PSAR']  = min(df['Low'][i - 1],df['Low'][i - 2],df['PSAR'][i])
            else:
                if df['Low'][i] < lp:
                    lp = df['Low'][i]
                    af = min(af + iaf, maxaf)
                df.loc[i,'PSAR'] = max(df['High'][i - 2],df['PSAR'][i],df['High'][i - 1])      
    return df