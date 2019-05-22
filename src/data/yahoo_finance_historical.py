
# -*- coding: utf-8 -*-

# Author: Jev Kuznetsov <jev.kuznetsov@gmail.com>
# License: BSD


"""
Toolset working with yahoo finance data
Module includes functions for easy access to YahooFinance data
"""



import urllib.request
import numpy as np

import requests # interaction with the web
import os  #  file system operations
import yaml # human-friendly data format
import re  # regular expressions
import pandas as pd # pandas... the best time series library out there
import datetime as dt # date and time functions
import io
import time

dateTimeFormat = "%Y%m%d %H:%M:%S"

def parseStr(s):
    ''' convert string to a float or string '''
    f = s.strip()
    if f[0] == '"':
        return f.strip('"')
    elif f=='N/A':
        return np.nan

    else:
        try: # try float conversion
            prefixes = {'M':1e6, 'B': 1e9}
            prefix = f[-1]

            if prefix in prefixes: # do we have a Billion/Million character?
                return float(f[:-1])*prefixes[prefix]
            else:                       # no, convert to float directly
                return float(f)
        except ValueError: # failed, return original string
            return s



def getQuote(symbols):
    """
    get current yahoo quote

    Parameters
    -----------
    symbols : list of str
        list of ticker symbols

    Returns
    -----------
    DataFrame , data is row-wise
    """

    # for codes see: http://www.gummy-stuff.org/Yahoo-data.htm
    if not isinstance(symbols,list):
        symbols = [symbols]


    header =               ['symbol','last','change_pct','PE','time','short_ratio','prev_close','eps','market_cap']
    request = str.join('', ['s',     'l1',     'p2'  ,   'r', 't1',     's7',        'p',       'e'     , 'j1'])


    data = dict(list(zip(header,[[] for i in range(len(header))])))

    urlStr = 'http://finance.yahoo.com/d/quotes.csv?s=%s&f=%s' % (str.join('+',symbols), request)

    try:
        lines = urllib.request.urlopen(urlStr).readlines()
    except Exception as e:
        s = "Failed to download:\n{0}".format(e);
        print(s)

    for line in lines:
        fields = line.decode().strip().split(',')
        #print fields, len(fields)
        for i,field in enumerate(fields):
            data[header[i]].append( parseStr(field))

    idx = data.pop('symbol')


    return pd.DataFrame(data,index=idx)


def getHistoricData(symbols, **options):
    '''
    get data from Yahoo finance and return pandas dataframe
    Will get OHLCV data frame if sinle symbol is provided.
    If many symbols are provided, it will return a wide panel

    Parameters
    ------------
    symbols : str or list
        Yahoo finanance symbol or a list of symbols
    sDate : tuple  (optional)
        start date (y,m,d)
    adjust : bool
        T/[F] adjust data based on adj_close

    Returns
    ---------
    Panel

    '''

    assert isinstance(symbols,(list,str)), 'Input must be a string symbol or a list of symbols'

    if isinstance(symbols,str):
        return getSymbolData(symbols,**options)
    else:
        data = {}
        print('Downloading data:')
        p = ProgressBar(len(symbols))
        for idx,symbol in enumerate(symbols):
            p.animate(idx+1)
            data[symbol] = getSymbolData(symbol,verbose=False,**options)

        return pd.Panel(data)

def getSymbolData(symbol, sDate=(2000,1,1), adjust=False, verbose=True):
    """
    get data from Yahoo finance and return pandas dataframe
    Parameters
    -----------
    symbol : str
        Yahoo finanance symbol
    sDate : tuple , optional
        start date (y,m,d), defaults to 1 jan 1990
    adjust : bool , optional
        use adjusted close values to correct OHLC. adj_close will be ommited
    verbose : bool , optional
        print output

    Returns
    ---------
        DataFrame

    """

    period1 = int(dt.datetime(*sDate).timestamp()) # convert to seconds since epoch
    period2 = int(dt.datetime.now().timestamp())
    cookie, crumb = get_cookie_crumb(symbol)
    params = (symbol, period1, period2, crumb)

    url = "https://query1.finance.yahoo.com/v7/finance/download/{0}?period1={1}&period2={2}&interval=1d&events=history&crumb={3}".format(*params)
    #print(url)
    data = requests.get(url, cookies=cookie)

    buf = io.StringIO(data.text) # create a buffer

    #print(buf.getvalue())
    #print(_token)
    df = pd.read_csv(buf,index_col=0,parse_dates=True) # convert to pandas DataFrame

    # rename columns
    newNames = [c.lower().replace(' ','_') for c in df.columns]
    renames = dict(zip(df.columns,newNames))
    df = df.rename(columns=renames)

    if verbose:
        print((symbol, 'Got %i days of data' % len(df)))

    if adjust:
        return _adjust(df,removeOrig=True)
    else:
        return df

def _adjust(df, removeOrig=False):
    '''
  _adjustust hist data based on adj_close field
    '''
    c = df['close']/df['adj_close']

    df['adj_open'] = df['open']/c
    df['adj_high'] = df['high']/c
    df['adj_low'] = df['low']/c

    if removeOrig:
        df=df.drop(['open','close','high','low'],axis=1)
        renames = dict(list(zip(['adj_open','adj_close','adj_high','adj_low'],['open','close','high','low'])))
        df=df.rename(columns=renames)

    return df


def loadToken(ticker="AAPL"):
    """
    get cookie and crumb from APPL page or disk.
    force = overwrite disk data
    """
    refreshDays = 30 # refreh cookie every x days

    # set destinatioin file
    dataDir = os.path.expanduser('~')+'/twpData'
    dataFile = dataFile = os.path.join(dataDir,'yahoo_cookie.yml')

    try : # load file from disk

        data = yaml.load(open(dataFile,'r'))
        age = (dt.datetime.now()- dt.datetime.strptime(  data['timestamp'], dateTimeFormat) ).days
        assert age < refreshDays, 'cookie too old'

    except (AssertionError,FileNotFoundError):     # file not found

        if not os.path.exists(dataDir):
            os.mkdir(dataDir)

        data = getToken(ticker, dataFile)

    return data


def getToken(ticker="AAPL", fName = None):
    """ get cookie and crumb from yahoo """

    url = 'https://uk.finance.yahoo.com/quote/{}/history'.format(ticker) # url for a ticker symbol, with a download link
    r = requests.get(url)  # download page

    txt = r.text # extract html

    cookie = r.cookies['B'] # the cooke we're looking for is named 'B'

    pattern = re.compile('.*"CrumbStore":\{"crumb":"(?P<crumb>[^"]+)"\}')

    for line in txt.splitlines():
        m = pattern.match(line)
        if m is not None:
            crumb = m.groupdict()['crumb']

    assert r.status_code == 200 # check for succesful download

    # save to disk
    data = {'crumb': crumb, 'cookie':cookie, 'timestamp':dt.datetime.now().strftime(dateTimeFormat)}

    if fName  is not None: # save to file
        with open(fName,'w') as fid:
            yaml.dump(data,fid)

    return data





########
def split_crumb_store(v):
    return v.split(':')[2].strip('"')


def find_crumb_store(lines):
    # Looking for
    # ,"CrumbStore":{"crumb":"9q.A4D1c.b9
    for l in lines:
        if re.findall(r'CrumbStore', l):
            return l
    print("Did not find CrumbStore")


def get_cookie_value(r):
    return {'B': r.cookies['B']}


def get_page_data(symbol):
    url = "https://finance.yahoo.com/quote/%s/?p=%s" % (symbol, symbol)
    r = requests.get(url)
    cookie = get_cookie_value(r)

    # Code to replace possible \u002F value
    # ,"CrumbStore":{"crumb":"FWP\u002F5EFll3U"
    # FWP\u002F5EFll3U
    lines = r.content.decode('unicode-escape').strip(). replace('}', '\n')
    return cookie, lines.split('\n')


def get_cookie_crumb(symbol):
    cookie, lines = get_page_data(symbol)
    crumb = split_crumb_store(find_crumb_store(lines))
    return cookie, crumb


def get_data(symbol, start_date, end_date, cookie, crumb):
    filename = '%s.csv' % (symbol)
    url = "https://query1.finance.yahoo.com/v7/finance/download/%s?period1=%s&period2=%s&interval=1d&events=history&crumb=%s" % (symbol, start_date, end_date, crumb)
    response = requests.get(url, cookies=cookie)
    with open (filename, 'wb') as handle:
        for block in response.iter_content(1024):
            handle.write(block)


def get_now_epoch():
    # @see https://www.linuxquestions.org/questions/programming-9/python-datetime-to-epoch-4175520007/#post5244109
    return int(time.time())


def download_quotes(symbol):
    start_date = 0
    end_date = get_now_epoch()
    cookie, crumb = get_cookie_crumb(symbol)
    get_data(symbol, start_date, end_date, cookie, crumb)



##########

#-------------- get token
#_token = loadToken() # get token from disk or yahoo

#--------------tests------------
# to be executed with pytest


def test_getToken():
    ''' download token '''
    getToken()


def test_initToken():
    ''' remove and get token '''
    dataDir = os.path.expanduser('~')+'/twpData'
    dataFile = dataFile = os.path.join(dataDir,'yahoo_cookie.yml')

    if os.path.exists(dataFile):
        os.remove(dataFile)

    loadToken()

    assert os.path.exists(dataFile)

def test_download():

    vxx = getSymbolData('SPY')

    assert len(vxx) > 4000



def crawl_all(tickers):
    def crawl(ticker):
        #try:
        data = getSymbolData(ticker)
        #print(data.columns.values, type(data.columns.values))
        if 'open' not in  data.columns.values:
        	print(ticker , "failed with ", data.columns.values )
        	return
        return data
    #except:
         #   print("error encountered for ", ticker,  " trying again...")
          #  return crawl(ticker)

    datas = map(crawl, tickers)
    return(pd.concat(datas, keys=tickers, names=['Ticker', 'Date']))
