import time

def tweet_to_timestamp(date):
    """
    Convert timestamp from twitter into datetime object
    """
    return time.strptime(date,'%a %b %d %H:%M:%S +0000 %Y')
