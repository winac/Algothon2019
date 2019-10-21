import quandl
import ipypb
import pandas as pd

quandl.ApiConfig.api_key = 'N/A'

FB_COLUMNS = ['fans', 'fan_post_count', 'admin_post_count',
              'admin_post_likes', 'admin_post_comments', 'admin_post_shares',
              'engagement_score', 'people_talking_about']

INS_COLUMNS = ['followers_count', 'posts_count', 'likes_count', 'comments_count',
               'total_posts_count', 'engagement_score']

TWTT_COLUMNS = ['followers_count', 'followees_count', 'tweets_count', 'retweets_count',
                'replies_count', 'favorites_count', 'total_tweets_count',
                'engagement_score']

def get_sentiment_data(stock, platform, date_range):

    function_mapper = {'fb':get_facebook_data,
                       'ins':get_instagram_data,
                       'twtt':get_twitter_data}

    data = function_mapper[platform](stock, date_range)

    return data


def get_facebook_data(stock, date_range):

    data, dates = [], []

    for date in ipypb.track(date_range):
    
        try:
            fb_sentiment_data = quandl.get_table('SMA/FBD', brand_ticker=stock, date=date.strftime('%Y-%m-%d'))
            fb_sentiment_data = fb_sentiment_data[FB_COLUMNS]
            fb_sentiment_data = fb_sentiment_data.groupby('fans', as_index=False).mean().sum()

            data.append(fb_sentiment_data)
            dates.append(date.date())
        except:
            continue

    fb_data = pd.DataFrame(data, index=dates)

    new_columns = [stock+'.'+column+'_fb' for column in fb_data.columns]
    fb_data = fb_data.rename(columns=dict(zip(fb_data.columns, new_columns)))

    return fb_data


def get_instagram_data(stock, date_range):

    data, dates = [], []

    for date in ipypb.track(date_range):
        try:
            ins_sentiment_data = quandl.get_table('SMA/INSD', brand_ticker=stock, date=date.strftime('%Y-%m-%d'))
            ins_sentiment_data = ins_sentiment_data[INS_COLUMNS]
            ins_sentiment_data = ins_sentiment_data.sum()

            data.append(ins_sentiment_data)
            dates.append(date)
        except:
            continue

    ins_data = pd.DataFrame(data, index=dates)

    new_columns = [stock+'.'+column+'_ins' for column in ins_data.columns]
    ins_data = ins_data.rename(columns=dict(zip(ins_data.columns, new_columns)))

    return ins_data


def get_twitter_data(stock, date_range):

    data, dates = [], []

    for date in ipypb.track(date_range):
        try:
            twtt_sentiment_data = quandl.get_table('SMA/TWTD', brand_ticker=stock, date=date.strftime('%Y-%m-%d'))
            twtt_sentiment_data = twtt_sentiment_data[TWTT_COLUMNS]
            twtt_sentiment_data = twtt_sentiment_data.sum()

            data.append(twtt_sentiment_data)
            dates.append(date)
        except:
            continue

    twtt_data = pd.DataFrame(data, index=dates)

    new_columns = [stock+'.'+column+'_twtt' for column in twtt_data.columns]
    twtt_data = twtt_data.rename(columns=dict(zip(twtt_data.columns, new_columns)))
    
    return twtt_data

