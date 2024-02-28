from absl import app

from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter
class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass

import yfinance as yf


def main(_):
    session = CachedLimiterSession(
        limiter=Limiter(RequestRate(2, Duration.SECOND*5)),  # max 2 requests per 5 seconds
        bucket_class=MemoryQueueBucket,
        backend=SQLiteCache("yfinance.cache"),
    )

    session.headers['User-agent'] = 'my-program/1.0'
    ticker = yf.Ticker('msft', session=session)
    breakpoint()
    tickers = yf.download(['msft', 'aapl'], period='1d')
    # The scraped response will be stored in the cache
    # print(ticker.actions)
    # print(ticker)

    # print(ticker.history(period='1mo'))
    # print(dir(ticker))
    # print(ticker.get_financials(freq='yearly'))
    # print(ticker.get_earnings())
    print(tickers)

    # msft = yf.Ticker("MSFT")

    # # get all stock info
    # msft.info

    # # get historical market data
    # hist = msft.history(period="1mo")


if __name__ == "__main__":
    app.run(main)
