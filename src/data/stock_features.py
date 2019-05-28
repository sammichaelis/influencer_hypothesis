# Stock Features

"""
Collection of stock-based features based on Yahoo Finance Data
"""
import pandas as pd


def daily_change(df):
    """
    Computes daily price change over 1 day

    Arguments:
    df -- yahoo finance dataframe

    Returns:
    new pandas series object of the same shape as df with computed daily percent change
    """
    return (df.open / df.adj_close) - 1



def k_day_percent_change(k, df):
    """
    Computes the k-day adjusted close percent change.
    Example: k=2, is the daily percent change

    Arguments:
    k --  number of days into past to use , k>1
    df -- yahoo finance dataframe. Must be indexed by data, and have columne adj_close
    """
    return (df.open / df.adj_close.shift(k)) - 1



def log_returns(k, df):
    """
    Computes the log returns over a k-day period

    Arguments:
    k -- window size
    df -- yahoo finance dataframe

    Returns:
    Log returns over a day period
    """
    return  np.log(df.open/ df.adj_close.shift(k))


def returns(k, df):
    """
    Computes the log returns over a k-day period

    Arguments:
    k -- window size
    df -- yahoo finance dataframe

    Returns:
    Log returns over a day period
    """
    return  ((df.open / df.adj_close.shift(k)) - 1) * 100


def compute_volatility(period, df, ret_key='monthly_returns'):
    """
    Computes the rolling volatility (standard deviation) of the log returns

    Arguments:
    preiod -- window (suggested size of 252)
    df -- yahoo finance dataframe
    ret_key -- string key to access the returns

    Returns:
    Volatility of a stock with a window of k
    """
    return df[ret_key].rolling(window=period).std() * np.sqrt(period)

def generate_target(forecast_period, df):
    """
    Comppute growth of a stock of a forecast period

    Arguments:
    forecast_period -- length of forecast (in days)
    df -- yahoo finance dataframe.

    Returns:
    feature vector of ground truth percent changes
    Exampls:
    """
    #return np.log((df.adj_close.shift(-1 * forecast_period) / df.adj_close ) )
    return (((df.adj_close.shift(-1 * forecast_period) / df.adj_close ) ) - 1)
