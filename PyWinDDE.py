# version 0.01

from ctypes import POINTER, WINFUNCTYPE, c_char_p, c_void_p, c_int, c_ulong, c_char_p
from ctypes.wintypes import BOOL, DWORD, BYTE, INT, LPCWSTR, UINT, ULONG
import time
import sqlite3
import datetime
import sys

# DECLARE_HANDLE(name) typedef void *name;
HCONV     = c_void_p  # = DECLARE_HANDLE(HCONV)
HDDEDATA  = c_void_p  # = DECLARE_HANDLE(HDDEDATA)
HSZ       = c_void_p  # = DECLARE_HANDLE(HSZ)
LPBYTE    = c_char_p  # POINTER(BYTE)
LPDWORD   = POINTER(DWORD)
LPSTR    = c_char_p
ULONG_PTR = c_ulong

# See windows/ddeml.h for declaration of struct CONVCONTEXT
PCONVCONTEXT = c_void_p

DMLERR_NO_ERROR = 0

# Predefined Clipboard Formats
CF_TEXT         =  1
CF_BITMAP       =  2
CF_METAFILEPICT =  3
CF_SYLK         =  4
CF_DIF          =  5
CF_TIFF         =  6
CF_OEMTEXT      =  7
CF_DIB          =  8
CF_PALETTE      =  9
CF_PENDATA      = 10
CF_RIFF         = 11
CF_WAVE         = 12
CF_UNICODETEXT  = 13
CF_ENHMETAFILE  = 14
CF_HDROP        = 15
CF_LOCALE       = 16
CF_DIBV5        = 17
CF_MAX          = 18

DDE_FACK          = 0x8000
DDE_FBUSY         = 0x4000
DDE_FDEFERUPD     = 0x4000
DDE_FACKREQ       = 0x8000
DDE_FRELEASE      = 0x2000
DDE_FREQUESTED    = 0x1000
DDE_FAPPSTATUS    = 0x00FF
DDE_FNOTPROCESSED = 0x0000

DDE_FACKRESERVED  = (~(DDE_FACK | DDE_FBUSY | DDE_FAPPSTATUS))
DDE_FADVRESERVED  = (~(DDE_FACKREQ | DDE_FDEFERUPD))
DDE_FDATRESERVED  = (~(DDE_FACKREQ | DDE_FRELEASE | DDE_FREQUESTED))
DDE_FPOKRESERVED  = (~(DDE_FRELEASE))

XTYPF_NOBLOCK        = 0x0002
XTYPF_NODATA         = 0x0004
XTYPF_ACKREQ         = 0x0008

XCLASS_MASK          = 0xFC00
XCLASS_BOOL          = 0x1000
XCLASS_DATA          = 0x2000
XCLASS_FLAGS         = 0x4000
XCLASS_NOTIFICATION  = 0x8000

XTYP_ERROR           = (0x0000 | XCLASS_NOTIFICATION | XTYPF_NOBLOCK)
XTYP_ADVDATA         = (0x0010 | XCLASS_FLAGS)
XTYP_ADVREQ          = (0x0020 | XCLASS_DATA | XTYPF_NOBLOCK)
XTYP_ADVSTART        = (0x0030 | XCLASS_BOOL)
XTYP_ADVSTOP         = (0x0040 | XCLASS_NOTIFICATION)
XTYP_EXECUTE         = (0x0050 | XCLASS_FLAGS)
XTYP_CONNECT         = (0x0060 | XCLASS_BOOL | XTYPF_NOBLOCK)
XTYP_CONNECT_CONFIRM = (0x0070 | XCLASS_NOTIFICATION | XTYPF_NOBLOCK)
XTYP_XACT_COMPLETE   = (0x0080 | XCLASS_NOTIFICATION )
XTYP_POKE            = (0x0090 | XCLASS_FLAGS)
XTYP_REGISTER        = (0x00A0 | XCLASS_NOTIFICATION | XTYPF_NOBLOCK )
XTYP_REQUEST         = (0x00B0 | XCLASS_DATA )
XTYP_DISCONNECT      = (0x00C0 | XCLASS_NOTIFICATION | XTYPF_NOBLOCK )
XTYP_UNREGISTER      = (0x00D0 | XCLASS_NOTIFICATION | XTYPF_NOBLOCK )
XTYP_WILDCONNECT     = (0x00E0 | XCLASS_DATA | XTYPF_NOBLOCK)
XTYP_MONITOR         = (0x00F0 | XCLASS_NOTIFICATION | XTYPF_NOBLOCK)

XTYP_MASK            = 0x00F0
XTYP_SHIFT           = 4

TIMEOUT_ASYNC        = 0xFFFFFFFF

def get_winfunc(libname, funcname, restype=None, argtypes=(), _libcache={}):
    """Retrieve a function from a library, and set the data types."""
    from ctypes import windll

    if libname not in _libcache:
        _libcache[libname] = windll.LoadLibrary(libname)
    func = getattr(_libcache[libname], funcname)
    func.argtypes = argtypes
    func.restype = restype

    return func

DDECALLBACK = WINFUNCTYPE(HDDEDATA, UINT, UINT, HCONV, HSZ, HSZ, HDDEDATA,ULONG_PTR, ULONG_PTR)
def Value2String(Insertrow):
    valueString = ''
    for col in range(len(Insertrow)):
        if col > 0:
            valueString += ','
        if type(Insertrow[col]) in [float,int]:
            valueString += str(Insertrow[col])
        elif type(Insertrow[col]) == datetime.datetime:
            valueString += str(unix_time_millis(Insertrow[col]))
        else:
            valueString += "'" + str(Insertrow[col]).replace("'",'"') +  "'"
    return valueString

def DBS(tblname):
    c.execute("""CREATE TABLE IF NOT EXISTS [tblname] (
        ID INT PRIMARY KEY     NOT NULL,
        Zero   REAL,
        Open           REAL,
        Close           REAL,
        High           REAL,
        Low           REAL,
        Volume           INT,
        Value           REAL)
        ;""".replace('[tblname]',tblname))
    conn.commit()

class DDE(object):
    """Object containing all the DDE functions"""
    AccessData         = get_winfunc("user32", "DdeAccessData",          LPBYTE,   (HDDEDATA, LPDWORD))
    ClientTransaction  = get_winfunc("user32", "DdeClientTransaction",   HDDEDATA, (LPBYTE, DWORD, HCONV, HSZ, UINT, UINT, DWORD, LPDWORD))
    Connect            = get_winfunc("user32", "DdeConnect",             HCONV,    (DWORD, HSZ, HSZ, PCONVCONTEXT))
    CreateStringHandle = get_winfunc("user32", "DdeCreateStringHandleW", HSZ,      (DWORD, LPCWSTR, UINT))
    Disconnect         = get_winfunc("user32", "DdeDisconnect",          BOOL,     (HCONV,))
    GetLastError       = get_winfunc("user32", "DdeGetLastError",        UINT,     (DWORD,))
    Initialize         = get_winfunc("user32", "DdeInitializeW",         UINT,     (LPDWORD, DDECALLBACK, DWORD, DWORD))
    FreeDataHandle     = get_winfunc("user32", "DdeFreeDataHandle",      BOOL,     (HDDEDATA,))
    FreeStringHandle   = get_winfunc("user32", "DdeFreeStringHandle",    BOOL,     (DWORD, HSZ))
    QueryString        = get_winfunc("user32", "DdeQueryStringW",        DWORD,    (DWORD, HSZ, LPSTR, DWORD, c_int))
    UnaccessData       = get_winfunc("user32", "DdeUnaccessData",        BOOL,     (HDDEDATA,))
    Uninitialize       = get_winfunc("user32", "DdeUninitialize",        BOOL,     (DWORD,))

class DDEError(RuntimeError):
    """Exception raise when a DDE errpr occures."""
    def __init__(self, msg, idInst=None):
        if idInst is None:
            RuntimeError.__init__(self, msg)
        else:
            RuntimeError.__init__(self, "{} (err={})".format(msg, hex(DDE.GetLastError(idInst))))

class DDEClient(object):
    """The DDEClient class.

    Use this class to create and manage a connection to a service/topic.  To get
    classbacks subclass DDEClient and overwrite callback."""

    def __init__(self, service, topic):
        """Create a connection to a service/topic."""
        from ctypes import byref

        self._idInst = DWORD(0)
        self._hConv = HCONV()

        self._callback = DDECALLBACK(self._callback)
        res = DDE.Initialize(byref(self._idInst), self._callback, 0x00000010, 0)
        if res != DMLERR_NO_ERROR:
            raise DDEError("Unable to register with DDEML (err={0})".format(hex(res)))

        hszService = DDE.CreateStringHandle(self._idInst, service, 1200)
        hszTopic = DDE.CreateStringHandle(self._idInst, topic, 1200)
        self._hConv = DDE.Connect(self._idInst, hszService, hszTopic, PCONVCONTEXT())
        DDE.FreeStringHandle(self._idInst, hszTopic)
        DDE.FreeStringHandle(self._idInst, hszService)
        if not self._hConv:
            raise DDEError("Unable to establish a conversation with server", self._idInst)
        self.callbackfun = {}

    def __del__(self):
        """Cleanup any active connections."""
        if self._hConv:
            DDE.Disconnect(self._hConv)
        if self._idInst:
            DDE.Uninitialize(self._idInst)

    def advise(self, item, callback=False,stop=False):
        """Request updates when DDE data changes."""
        from ctypes import byref

        hszItem = DDE.CreateStringHandle(self._idInst, item, 1200)
        hDdeData = DDE.ClientTransaction(LPBYTE(), 0, self._hConv, hszItem, CF_TEXT, XTYP_ADVSTOP if stop else XTYP_ADVSTART, TIMEOUT_ASYNC, LPDWORD())
        DDE.FreeStringHandle(self._idInst, hszItem)
        if not hDdeData:
            raise DDEError("Unable to {} advise".format("stop" if stop else "start"), self._idInst)
        DDE.FreeDataHandle(hDdeData)
        if callback is False:
            self.callbackfun[item] = self.printDDE
        else:
            self.callbackfun[item] = callback

    def execute(self, command, timeout=5000):
        """Execute a DDE command."""
        pData = c_char_p(command)
        cbData = DWORD(len(command) + 1)
        hDdeData = DDE.ClientTransaction(pData, cbData, self._hConv, HSZ(), CF_TEXT, XTYP_EXECUTE, timeout, LPDWORD())
        if not hDdeData:
            raise DDEError("Unable to send command", self._idInst)
        DDE.FreeDataHandle(hDdeData)

    def request(self, item, timeout=5000):
        """Request data from DDE service."""
        from ctypes import byref

        hszItem = DDE.CreateStringHandle(self._idInst, item, 1200)
        hDdeData = DDE.ClientTransaction(LPBYTE(), 0, self._hConv, hszItem, CF_TEXT, XTYP_REQUEST, timeout, LPDWORD())
        DDE.FreeStringHandle(self._idInst, hszItem)
        if not hDdeData:
            raise DDEError("Unable to request item", self._idInst)

        if timeout != TIMEOUT_ASYNC:
            pdwSize = DWORD(0)
            pData = DDE.AccessData(hDdeData, byref(pdwSize))
            if not pData:
                DDE.FreeDataHandle(hDdeData)
                raise DDEError("Unable to access data", self._idInst)
            # TODO: use pdwSize
            DDE.UnaccessData(hDdeData)
        else:
            pData = None
            DDE.FreeDataHandle(hDdeData)
        return pData

    def callback(self, value, item=None):
        """Calback function for advice."""
        self.callbackfun[item.decode(errors='ignore')](value.decode(errors='ignore'),item.decode(errors='ignore'))

    def printDDE(self, value, item=None):
        print("{}:{}".format(item,value))


    def _callback(self, wType, uFmt, hConv, hsz1, hsz2, hDdeData, dwData1, dwData2):
        #if wType == XTYP_ADVDATA:
        from ctypes import byref, create_string_buffer

        dwSize = DWORD(0)
        pData = DDE.AccessData(hDdeData, byref(dwSize))
        if pData:
            item = create_string_buffer(b'\000' * 128)
            DDE.QueryString(self._idInst, hsz2, item, 128, 1004)
            self.callback(pData, item.value)
            DDE.UnaccessData(hDdeData)
            return DDE_FACK
        return 0

def WinMSGLoop():
    """Run the main windows message loop."""
    from ctypes import POINTER, byref, c_ulong
    from ctypes.wintypes import BOOL, HWND, MSG, UINT

    LPMSG = POINTER(MSG)
    LRESULT = c_ulong
    GetMessage = get_winfunc("user32", "GetMessageW", BOOL, (LPMSG, HWND, UINT, UINT))
    TranslateMessage = get_winfunc("user32", "TranslateMessage", BOOL, (LPMSG,))
    # restype = LRESULT
    DispatchMessage = get_winfunc("user32", "DispatchMessageW", LRESULT, (LPMSG,))
    msg = MSG()
    lpmsg = byref(msg)
    if GetMessage(lpmsg, HWND(), 0, 0) > 0:
##        print GetMessage(lpmsg, HWND(), 0, 0)
        TranslateMessage(lpmsg)
        DispatchMessage(lpmsg)
        
        
def recTickData(value,item):
    sqlTemplate = """
                    INSERT OR REPLACE INTO [tblName]
                    (ID, Zero,Open,Close,High,Low ,Volume,Value)
                    VALUES ( [InsertValue] )
                """
    [tblname,Close,Zero,Volume,High,Low,Open,Value] = ['' for i in range(8)]
    [tblname,Close,Zero,Volume,High,Low,Open,Value] = ['' for i in range(8)]
    # ID,TradingDate,Time,Price,High,Low,Open,PriceChange,TotalVolume,Value,Volume,EstimatedTotalVolume
    Data = str(value).split(';')
##    print '\r',datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') ,Data,
    if not '--' in Data: 
        Close = float(Data[3])
        Volume = int(float(Data[11].replace('\x02','')))
        High = float(Data[4])
        Low = float(Data[5])
        Open = float(Data[6])
        Value = int(float(Data[9]))
        Zero = float(Data[7].replace('+',''))
        tID = float(Data[0])
        tblname = 'tbl' + datetime.datetime.strptime(Data[1],'%Y/%m/%d').strftime('%Y%m%d')
    
    if tblname and Zero and Close:
        Zero = Close - Zero
        Insertrow = [int(tID),Zero,Open,Close,High,Low ,Volume,Value]
        print '\r',datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') ,tblname,Insertrow,
        c.execute(sqlTemplate.replace('[tblName]',tblname).replace('[InsertValue]',Value2String(Insertrow)))
        
if __name__ == "__main__":
    DBpath = 'Local.db'
    conn = sqlite3.connect(DBpath, check_same_thread=False)
    c = conn.cursor()
    
    # Create a connection to ESOTS (OTS Swardfish) and to instrument MAR11 ALSI
    print 'Create a connection'
    while True:
        try:
##            dde = DDEClient("XQKGIAP","Quote")
            dde = DDEClient("XQLITE","Quote")
            print "Sucessfully Connect to DDE server ..."
            break
        except:
            e = sys.exc_info()[0]
            print "Error: %s, try to connect 10 mins later."%e
            time.sleep(600)
            print "Connect to DDE server again..."
    tblList = []
    print 'Query TableList'
    cursor = c.execute("""SELECT name FROM sqlite_master WHERE type='table';""")
    for row in cursor:
        if len(str(row[0])) == 7:
            ID = str(row[0]).replace('tbl','')
            tblList.append(ID)
            # Monitor the various attributes from MAR11 ALSI
    conn.close()
    print 'Start Listening'
    DBpath = 'Realtime.db'
    conn = sqlite3.connect(DBpath, check_same_thread=False)
    c = conn.cursor()
    tblname = 'tbl'+datetime.datetime.now().strftime('%Y%m%d')
    DBS(tblname)
    # Run the main message loop to receive advices
    while datetime.datetime.now().hour < 13:
        for ID in sorted(tblList):
##            dde.advise(ID + ".TW-ID,TradingDate,Price,PriceChange,TotalVolume,High,Low,Open,Value,Volume",callback = recTickData)
            dde.advise(ID + ".TW-ID,TradingDate,Time,Price,High,Low,Open,PriceChange,TotalVolume,Value,Volume,EstimatedTotalVolume,Open",callback = recTickData)
            # XQLITE|Quote!'2448'
            WinMSGLoop()
        conn.commit()
        time.sleep(3)
    conn.close()
       
