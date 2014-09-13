import logging
import http.client
import urllib
import urllib.error
import urllib.request
import re
import socket
import pandas.io.data
import numpy as np
from datetime import date, datetime
from time import sleep

"""Lookup table for FRED symbols"""
fred_currencies = {
    "JPY": "DEXJPUS",
    "EUR": "DEXUSEU",
    "MXN": "DEXMXUS",
    "GBP": "DEXUSUK",
    "CAD": "DEXCAUS",
    "AUD": "DEXUSAL",
    "CHF": "DEXSZUS",
    "HKD": "DEXHKUS",
    "ZAR": "DEXSFUS",
    "SEK": "DEXSDUS",
    "SGD": "DEXSIUS",
    "NOK": "DEXNOUS",
    "DKK": "DEXDNUS"
}

"""Lookup table for Yahoo stats symbols"""
yahoo_stats = {
    "name": "n",
    "price": "l1",
    "change": "c1",
    "change_pct": "p2",
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


def dl(symbol, timeout=600):
    """
    Download yahoo bars for symbol. Return results as str.
    Arguments:
    symbol   -- symbol to download
    timeout  -- how many seconds to wait until timeout (default: 600)
    """
    start_date = date(year=1900, month=1, day=1)
    end_date = date(year=2019, month=12, day=31)
    while True:
        try:
            hist_data = dl_raw(symbol, start_date, end_date, 'd', 'csv', timeout)
            break
        except urllib.error.HTTPError as err:
            if err.msg == "Server Hangup":
                logging.debug("Server hanged up while downloading historical data for" +
                              " {}, retrying in 5 seconds...".format(symbol))
                sleep(5)
                continue
            if err.msg == "Not Found":
                logging.warning("Cannot find historical data for {} from Yahoo.".format(symbol))
                return None
            else:
                raise
        except (socket.timeout, urllib.error.URLError, ConnectionResetError,
                http.client.IncompleteRead):
            logging.debug("Connection problem while downloading historical data for" +
                          " {}, retrying in 5 seconds...".format(symbol))
            sleep(5)
            continue
    while True:
        try:
            misc_info = dl_mainpage(symbol, timeout)
            break
        except urllib.error.HTTPError as err:
            if err.msg == "Internal Server Error" or err.msg == "Server Hangup":
                logging.debug("{} while downloading mainpage for symbol".format(err.msg) +
                              " {}, retrying in 5 seconds...".format(symbol))
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
        logging.warning("Cannot find currency for {}.".format(symbol))
        return None
    lines = hist_data.splitlines()
    lines[0] = lines[0].replace(" ", "")
    lines[0] += "({})".format(misc_info["currency"])
    return "\n".join(lines)


# DEPRECATED!
# def dl_csvbased(symbol, dl_directory):
#     """
#     Downloads yahoo data.
#     return values:
#         "Downloaded" = File was downloaded
#         "Up-to-date" = File was up-to-date
#         "Not Found" = Symbol not found
#     """
#     filename = os.path.join(dl_directory, symbol + ".csv")
#     if not os.path.isfile(filename):
#         start_date = date(year=1900, month=1, day=1)
#         end_date = date(year=2019, month=12, day=31)
#         while True:
#             try:
#                 hist_data = dl_raw(symbol, start_date, end_date, 'd', 'csv')
#                 break
#             except urllib.error.HTTPError as err:
#                 if err.msg == "Server Hangup":
#                     logging.debug("Server hanged up while downloading historical data for" +
#                                   " {}, retrying in 5 seconds...".format(symbol))
#                     sleep(5)
#                     continue
#                 if err.msg == "Not Found":
#                     return err.msg
#                 else:
#                     raise
#             except socket.timeout:
#                 logging.debug("Connection timed out while downloading historical data for" +
#                               " {}, retrying in 5 seconds...".format(symbol))
#                 sleep(5)
#                 continue
#     else:
#         return "Up-to-date"
#
#     while True:
#         try:
#             misc_info = dl_mainpage(symbol)
#             break
#         except urllib.error.HTTPError as err:
#             if err.msg == "Internal Server Error" or err.msg == "Server Hangup":
#                 logging.debug("{} while downloading mainpage for symbol" +
#                               " {}, retrying in 5 seconds...".format(err.msg, symbol))
#                 sleep(5)
#                 continue
#             else:
#                 raise
#         except socket.timeout:
#                 logging.debug("Connection timed out while downloading mainpage for" +
#                               " {}, retrying in 5 seconds...".format(symbol))
#                 sleep(5)
#                 continue
#
#     if not misc_info["currency"]:
#         return "Not Found"
#
#     lines = hist_data.splitlines()
#     lines[0] = lines[0].replace(" ", "")
#     lines[0] += "({})".format(misc_info["currency"])
#
#     with open(filename, 'w') as csvfile:
#         for line in lines:
#             csvfile.write(line + "\n")
#
#     return "Downloaded"


def dl_raw(symbol, startdate, enddate, datatype, fmt, timeout):
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
        urltodl = "http://ichart.finance.yahoo.com/table.csv?"
    elif fmt == "x":
        urltodl = "http://ichart.finance.yahoo.com/x?"
    else:
        print("format ({}) not supported.".format(fmt))
        return
    urltodl += "s={symbol}".format(symbol=symbol)
    urltodl += "&a={startm}".format(startm=str(sd.month - 1).rjust(2, "0"))
    urltodl += "&b={startd}&c={starty}".format(startd=str(sd.day).rjust(2, "0"), starty=sd.year)
    urltodl += "&d={endm}".format(endm=str(ed.month - 1).rjust(2, "0"))
    urltodl += "&e={endd}&f={endy}&g={type}".format(endd=str(ed.day).rjust(2, "0"), endy=ed.year, type=datatype)
    logging.debug("Downloading historical Yahoo data from: {}".format(urltodl))
    req = urllib.request.urlopen(urltodl, timeout=timeout)
    data = req.read()
    datastr = data.decode()
    return datastr


def dl_mainpage(symbol, timeout):
    """
    Download data from the main Yahoo Finance page. Return dict of fetched data.

    Arguments:
    symbol   -- symbol to download
    timeout  -- how many seconds to wait until timeout
    """
    urltodl = "http://finance.yahoo.com/q?s={}".format(symbol)
    logging.debug("Downloading data from: {}".format(urltodl))
    req = urllib.request.urlopen(urltodl, timeout=timeout)
    data = req.read()
    datastr = data.decode()
    m = re.search("Currency in ...[.]", datastr)
    currency = None
    if m:
        currency = m.string[m.end()-4:m.end()-1].upper()
    results = {"currency": currency}
    return results


def convert_to_usd(files):
    """
    Convert OHLCVC files to USD (currency needs to be defined in header).

    Returns number of files converted.

    Arguments:
    files  -- files to convert
    """

    start = datetime(1900, 1, 1)
    end = datetime(2020, 1, 1)

    num_converted = 0

    # converted to XXX/USD
    fxrates = {}

    if type(files) is not list:
        files = [files]

    for filename in files:

        with open(filename, 'r') as csvfile:
            lines = csvfile.readlines()

        # already converted / conversion not required
        if "USD" in lines[0]:
            continue

        for i in range(len(lines)):
            lines[i] = lines[i].strip()

        m = re.search("[(]...[)]", lines[0])
        currency = m.string[m.end()-4:m.end()-1]

        if not m:
            logging.error("{} doesn't have currency specification.".format(filename))
            continue

        logging.debug("Converting to USD: {}...".format(filename))

        if currency not in fxrates:
            fredsymbol = fred_currencies[currency]
            logging.debug("Getting forex data for {} (FRED: {})...".format(currency, fredsymbol))
            data = pandas.io.data.DataReader(fredsymbol, "fred", start, end)
            if fredsymbol[-2:] == "US":
                logging.debug("Converting to {}/USD".format(currency))
                for i in range(len(data[fredsymbol])):
                    data[fredsymbol][i] = 1 / data[fredsymbol][i]
            data = data[np.isfinite(data[fredsymbol])]
            fxrates[currency] = data

        lines[0] += ",AdjClose(USD)"
        fxrate = fxrates[currency]

        for i in range(len(lines)-1, 0, -1):
            splitted = lines[i].split(',')
            adjclose = float(splitted[-1])
            dt = splitted[0]
            if dt in fxrate.index:
                lines[i] += "," + str(round(float(fxrate.ix[dt]) * adjclose, 6))
            else:
                logging.log(5, "Line removed from {}".format(dt))
                del lines[i]

        with open(filename, 'w') as csvfile:
            for line in lines:
                csvfile.write(line + "\n")

        num_converted += 1
        logging.debug("Converted to USD {}.".format(filename))

    return num_converted


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
        stats = list(yahoo_stats.keys())
    for stat in stats:
        s += yahoo_stats[stat]
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
