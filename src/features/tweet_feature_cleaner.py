import time
import calendar

def tweet_to_timestamp(date):
    """
    Convert timestamp from twitter into datetime object
    """
    ts  = time.strptime(date,'%a %b %d %H:%M:%S +0000 %Y')
    return calendar.timegm(ts)


def test_time():
    return time.time()
