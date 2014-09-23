import logging
import http.client
import urllib
import urllib.error
import urllib.request
import urllib3
import re
import io
import socket
import pandas as pd
from datetime import date
from time import sleep


"""Lookup table for Yahoo stats symbols"""
YAHOO_STATS = {
    "name": "n",
    "price": "l1",
    "change": "c1",
    "change_pct": "p2",
    "bid": "b3",
    "ask": "b2",
    "after_hours_change": "c8",
    "volume": "v",
    "avg_daily_volume": "a2",
    "exchange": "x",
    "market_cap": "j1",
    "book_value": "b4",
    "ebitda": "j4",
    "dividend_per_share": "d",
    "dividend_yield": "y",
    "dividend_pay_date": "r1",
    "ex_dividend_date": "q",
    "earnings_per_share": "e",
    "52_week_high": "k",
    "52_week_low": "j",
    "high": "h",
    "low": "g",
    "open": "o",
    "last": "l",
    "previous_close": "p",
    "50_day_sma": "m3",
    "200_day_sma": "m4",
    "change_from_200_day_sma": "m5",
    "pct_change_from_200_day_sma": "m6",
    "change_from_50_day_sma": "m7",
    "pct_change_from_50_day_sma": "m8",
    "pe_ratio": "r",
    "peg_ratio": "r5",
    "ps_ratio": "p5",
    "pb_ratio": "p6",
    "short_ratio": "s7",
    "last_trade_date": "d1",
    "notes": "n4",
    "error_indication": "e1"
}


YAHOO_HIST_DATA_HOST = 'ichart.finance.yahoo.com'
YAHOO_FINANCE_HOST = 'finance.yahoo.com' 


class YahooDataNotFoundException(Exception):
    pass


class CurrencyNotFoundException(Exception):
    pass


class InvalidDataException(Exception):
    pass


g_cp_historical = urllib3.HTTPConnectionPool(YAHOO_HIST_DATA_HOST, maxsize=1, block=True)
g_cp_main = urllib3.HTTPConnectionPool(YAHOO_FINANCE_HOST, maxsize=1, block=True)


def configure_downloader(threads, blocking=None):
    global g_cp_main
    global g_cp_historical
    if not blocking:
        if threads == 1:
            blocking = False
        else:
            blocking = True
    g_cp_historical = urllib3.HTTPConnectionPool(YAHOO_HIST_DATA_HOST, maxsize=threads, block=blocking)
    g_cp_main = urllib3.HTTPConnectionPool(YAHOO_FINANCE_HOST, maxsize=threads, block=blocking)


def dl(symbol, currency=None, timeout=600):
    """
    Download yahoo bars for symbol. Return data as pd.DataFrame.

    Currency is embedded on the AdjClose column. ( i.e. AdjClose(USD) )

    Arguments:
    symbol   -- symbol to download
    currency -- currency of the symbol
                (if not provided will be fetched from Yahoo)
    timeout  -- Seconds to wait per trial. Will keep on retrying on failure.
                (default: 60)

    Exceptions:
    YahooDataNotFoundException    -- no yahoo data for the symbol
    CurrencyNotFoundException     -- no currency was found for the symbol
    """
    start_date = date(year=1900, month=1, day=1)
    end_date = date(year=2019, month=12, day=31)
    while True:
        try:
            raw_data = dl_raw(symbol, start_date, end_date, 'd', 'csv', timeout=timeout)
            break
        except urllib.error.HTTPError as err:
            if err.msg == "Server Hangup":
                logging.debug("Server hanged up while downloading historical data for" +
                              " {}, retrying in 5 seconds...".format(symbol))
                err.fp.close()  # close the file descriptor/socket
                sleep(5)
                continue
            if err.msg == "Not Found":
                # logging.warning("Cannot find historical data for {} from Yahoo.".format(symbol))
                raise YahooDataNotFoundException(symbol)
            else:
                raise
        except (socket.timeout, urllib.error.URLError, ConnectionResetError,
                http.client.IncompleteRead):
            logging.debug("Connection problem while downloading historical data for" +
                          " {}, retrying in 5 seconds...".format(symbol))
            sleep(5)
            continue
    try:
        df = pd.DataFrame.from_csv(io.StringIO(raw_data))
    except:
        raise InvalidDataException(raw_data) 
    # we want ascending data order
    df = df.reindex(index=df.index[::-1])
    if not currency:
        # fetch currency from yahoo
        while True:
            try:
                misc_info = dl_mainpage(symbol, timeout)
                break
            except urllib.error.HTTPError as err:
                if err.msg == "Internal Server Error" or err.msg == "Server Hangup":
                    logging.debug("{} while downloading mainpage for symbol".format(err.msg) +
                                  " {}, retrying in 5 seconds...".format(symbol))
                    err.fp.close()  # close the file descriptor/socket
                    sleep(5)
                    continue
                else:
                    raise
            except (socket.timeout, urllib.error.URLError, ConnectionResetError):
                logging.debug("Connection timed out while downloading historical data for" +
                              " {}, retrying in 5 seconds...".format(symbol))
                sleep(5)
                continue
        if not misc_info["currency"]:
            raise CurrencyNotFoundException(symbol)
        currency = misc_info['currency']
    cols = df.columns.tolist()
    cols[-1] = "AdjClose({})".format(currency)
    df.columns = cols
    return df


def dl_raw(symbol, startdate, enddate, datatype, fmt, timeout=60):
    """Download raw data from Yahoo Finance.

    Arguments:
    symbol     -- symbol to download
    startdate  -- starting date for data
    enddate    -- ending date for data
    datatype   -- type of data to download
    fmt        -- formatting for the data to download
    timeout    -- how many seconds to wait until timeout
    """
    sd = startdate
    ed = enddate
    if fmt == "csv":
        # urltodl = "http://98.139.183.24/table.csv?"
        url = "/table.csv"
    elif fmt == "x":
        url = "/x"
    else:
        raise Exception("format ({}) not supported.".format(fmt))
    fields = {}
    fields['s'] = symbol
    fields['a'] = str(sd.month - 1).rjust(2, "0")
    fields['b'] = str(sd.day).rjust(2, "0")
    fields['c'] = sd.year
    fields['d'] = str(ed.month - 1).rjust(2, "0")
    fields['e'] = str(ed.day).rjust(2, "0")
    fields['f'] = ed.year
    fields['g'] = datatype
    r = g_cp_historical.request('GET', url, fields=fields)
    if r.status == 200:  # OK
        return r.data.decode()
    if r.status == 404:  # Not found
        raise YahooDataNotFoundException(symbol)
    import ipdb; ipdb.set_trace()
    # investigate unknown status code
    # req = urllib.request.urlopen(urltodl, timeout=timeout)
    # data = req.read()
    # datastr = data.decode()
    # return datastr


def dl_mainpage(symbol, timeout=60):
    """
    Download data from the main Yahoo Finance page. Return dict of fetched data.

    Arguments:
    symbol   -- symbol to download
    timeout  -- how many seconds to wait until timeout
    """
    # urltodl = "http://finance.yahoo.com/q?s={}".format(symbol)
    r = g_cp_main.request('GET', '/q', fields={'s': symbol})
    if r.status == 200:  # OK
        datastr = r.data.decode()
    elif r.status == 404:  # Not found
        raise YahooDataNotFoundException(symbol)
    else:
        import ipdb; ipdb.set_trace()
    m = re.search("Currency in ...[.]", datastr)
    currency = None
    if m:
        currency = m.string[m.end()-4:m.end()-1].upper()
    rults = {"currency": currency}
    return rults


def si_suffix_to_float(s):
    try:
        return float(s)
    except ValueError:
        suffix = s[-1]
        val = float(s[:-1])
        if suffix == 'K':
            return val * 1e3
        if suffix == 'M':
            return val * 1e6
        if suffix == 'B':
            return val * 1e9
        if suffix == 'T':
            return val * 1e12
        raise Exception("Suffix {} not recognized (str-form: {})".format(suffix, s))


def get_stats(symbol, stats, timeout=60):
    """Download stats from Yahoo Finance.

    Returns a dictionary containing results.

    Arguments:
    symbol   -- symbol to download stats for
    stats    -- list of stats to download (list of str)
    timeout  -- seconds to wait until timeout (default: 60)
    """
    if type(stats) is str:
        stats = [stats]
    s = ""
    if "all" in stats:
        stats = list(YAHOO_STATS.keys())
    for stat in stats:
        s += YAHOO_STATS[stat]
    url = 'http://finance.yahoo.com/d/quotes.csv?s={}&f={}'.format(symbol, s)
    req = urllib.request.urlopen(url, timeout=timeout)
    data = req.read().decode().strip().strip('"').split(',')
    res = {}
    for i in range(len(stats)):
        if stats[i] == 'ebitda' or stats[i] == 'market_cap':
            res[stats[i]] = si_suffix_to_float(data[i])
            continue
        try:
            # try parsing to float first
            res[stats[i]] = float(data[i])
        except ValueError:
            res[stats[i]] = data[i]
    return res
