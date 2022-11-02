from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from prefect.filesystems import GCS
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_load_file
from google.cloud import bigquery
from bs4 import BeautifulSoup
from datetime import datetime
import yfinance as yf
import ftplib
import urllib
import pandas as pd
import numpy as npgit
import io
import os
import requests
import pytz


# https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
pd.options.mode.chained_assignment = None  # default='warn'


gcp_credentials_block = GcpCredentials.load("creds-finarc")

DATA_PATH = "/home/admin/python/prefect/gme_price/data/"
USERNAME = "shortstock"
gcs_block = GCS.load("python-predictions")


@flow(task_runner=SequentialTaskRunner)
def push_to_bq(filepath):
    service_account_file = "/home/admin/python/prefect/gme_borrow_fee/finarc-180f85e870ac.json"
    gcp_credentials = GcpCredentials(
        service_account_file=service_account_file
    )

    result = bigquery_load_file(
        dataset="gme",
        table="borrow_rate",
        path=filepath,
        location='us-central1',
        gcp_credentials=gcp_credentials
    )


@task
def get_data(start_date, end_date):
    ticker = yf.download( 
                    start_date=start_date,
                    end_date=end_date,
                    tickers="GME",
                    interval="1h",
                    ignore_tz=False,
                    auto_adjust=True,
                    prepost=True,
                    proxy=None
                )

    df = pd.DataFrame(ticker)
    df.reset_index(inplace=True)
    df = df.rename({'Datetime':'DATETIME', 'Open':'OPEN', 'High':'HIGH', 'Low':'LOW', 'Close':'CLOSE', 'Volume':'VOLUME'}, axis=1)
    df['SYNCTIME'] = datetime.now()
    df['SYM'] = 'GME'

    return df


@task
def archive_data(filepath, data):
    try:
        data.to_csv(filepath, mode='w', index=False, header=True)
        return True
    except:
        return False


@flow(task_runner=SequentialTaskRunner)
def historical_price_pull():
    """
    This flow dowloads all historical pricing data from yahoo finance in 5m increments
    """
    logger = get_run_logger()

    periods = pd.period_range(start='2020-01-01', end='2022-11-01', freq="M")

    temp = ''

    for date in periods:
        if temp == '':
            temp = date
        else:
            filepath = f'{DATA_PATH}archive/YF-{temp}-{date}.csv'

            data = get_data(temp, date)

            result = archive_data(filepath, data)

            if result == True:
                push_to_bq(filepath)

            temp = date


if __name__ == "__main__":
    historical_price_pull()



