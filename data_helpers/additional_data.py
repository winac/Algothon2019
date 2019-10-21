import pandas as pd
import json

from google.cloud import bigquery
from google.oauth2 import service_account

personal_cred = json.loads('N/A')
proj = u'tr-data-workbench'
cred = service_account.Credentials.from_service_account_info(personal_cred)

keep_columns = [u'windowTimestamp', u'dataType', u'buzz', u'sentiment',
                u'optimism', u'joy', u'loveHate', u'trust', u'anger', u'conflict', u'fear', u'gloom',
                u'stress', u'surprise', u'timeUrgency', u'uncertainty', u'violence',
                u'emotionVsFact', u'volatility']

def get_weather_data():

    df = pd.read_csv("data/StormEvents_details-ftp_v1.0_d2014_c20180718.csv")
    df1 = pd.read_csv("data/StormEvents_details-ftp_v1.0_d2015_c20190817.csv")
    df2 = pd.read_csv("data/StormEvents_details-ftp_v1.0_d2016_c20190817.csv")
    df3 = pd.read_csv("data/StormEvents_details-ftp_v1.0_d2017_c20190817.csv")
    df4 = pd.read_csv("data/StormEvents_details-ftp_v1.0_d2018_c20191016.csv")
    df5 = pd.read_csv("data/StormEvents_details-ftp_v1.0_d2019_c20191016.csv")

    result = pd.concat([df, df1, df2, df3, df4, df5])

    result = result.filter(
        ["BEGIN_YEARMONTH", "BEGIN_DAY", "END_YEARMONTH", "EVENT_TYPE", "SOURCE", "MAGNITUDE", "END_DAY", "BEGIN_LAT",
         "BEGIN_LON", "END_LAT", "END_LON"]).drop_duplicates()

    result['begin_date'] = result["BEGIN_YEARMONTH"].map(str) + result["BEGIN_DAY"].map(str)
    result['end_date'] = result["END_YEARMONTH"].map(str) + result["END_DAY"].map(str)

    result['begin_date'] = pd.to_datetime(result['begin_date'].map(str), format='%Y%m%d')
    result['end_date'] = pd.to_datetime(result['end_date'].map(str), format='%Y%m%d')

    result = result.fillna(0)

    del result["END_YEARMONTH"]
    del result["END_DAY"]

    del result["BEGIN_YEARMONTH"]
    del result["BEGIN_DAY"]

    result["duration"] = result.end_date - result.begin_date

    event_type = pd.get_dummies(result['EVENT_TYPE'])

    result = result.join(event_type)

    result = result.drop('EVENT_TYPE', axis=1)
    result = result.drop('SOURCE', axis=1)

    result = result.drop("BEGIN_LAT", axis=1)
    result = result.drop("BEGIN_LON", axis=1)
    result = result.drop("END_LAT", axis=1)
    result = result.drop("END_LON", axis=1)
    result = result.drop("end_date", axis=1)

    result = result.groupby(["begin_date"]).sum()

    return result


def get_reaction_data(ticker):

    df = filterColumns(ticker)
    df = replaceNoneWithZero(df)
    df = datetimeToDate(df)

    news, news_social, social = splitByDataType(df)
    news, news_social, social = dropDataTypeColumn(news, news_social, social)
    news, news_social, social = aggregateDataByMean(news, news_social, social)

    return news, news_social, social

def pullData(ticker):
    query_minute = """
        SELECT 
            * 
        FROM 
            `tr-data-workbench.TRMI.daily` 
        WHERE 
            ticker = @ticker
        LIMIT 5000
    """

    query_params = [
        bigquery.ScalarQueryParameter('ticker', 'STRING', ticker)
    ]

    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params
    job_config.use_legacy_sql = False
    bigquery_client = bigquery.Client(project=proj, credentials=cred)
    df = bigquery_client.query(query_minute, job_config=job_config).to_dataframe()
    return df


def filterColumns(df):
    return df[keep_columns]


def datetimeToDate(df):
    df['windowTimestamp'] = df['windowTimestamp'].apply(lambda x: x.date())
    return df


def replaceNoneWithZero(df):
    return df.fillna(value=0)


def splitByDataType(df):
    news = df[df['dataType'] == u'News']
    news_social = df[df['dataType'] == u'News_Social']
    social = df[df['dataType'] == u'Social']
    return news, news_social, social


def dropDataTypeColumn(news, news_social, social):
    return news.drop('dataType', axis=1), news_social.drop('dataType', axis=1), social.drop('dataType', axis=1)


def aggregateDataByMean(news, news_social, social):
    news = news.set_index('windowTimestamp').astype(float).groupby('windowTimestamp').mean()
    news_social = news_social.set_index('windowTimestamp').astype(float).groupby('windowTimestamp').mean()
    social = social.set_index('windowTimestamp').astype(float).groupby('windowTimestamp').mean()
    return news, news_social, social


