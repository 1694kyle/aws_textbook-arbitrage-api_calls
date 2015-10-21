from amazonproduct.api import API
from amazonproduct.errors import AWSError
import boto
from boto.s3.key import Key
from datetime import datetime
import re
import pandas as pd
import numpy as np
import time


def item_keys(keys):
    regex = re.compile(r'scraping_items\/items-(.+)\.csv')
    return [key for key in keys if re.match(regex, key.name)] # and key.is_latest]


def upload_results(frame):
    if len(frame) == 0:
        return None
    k = Key(bucket)
    k.key = '/api_results/results-{}'.format(latest_items_key.name[latest_items_key.name.index('items-') + len('items-'):])
    k.set_contents_from_string(frame.to_csv())


def get_item_frame():
    frame = pd.DataFrame.from_csv(latest_items_key)
    for col in api_cols:
        frame[col] = np.nan
    return frame


def get_price_data(item_frame):
    for chunk in _chunker(item_frame, 10):
        asins = [row['asin'] for i, row in chunk.iterrows()]
        response = amzn_search(asins)
        for item in response.Items.Item:
            asin = item.ASIN
            if hasattr(item.ItemAttributes, 'IsEligibleForTradeIn'):
                trade_in_eligible = bool(item.ItemAttributes.IsEligibleForTradeIn)

                if trade_in_eligible:
                    if hasattr(item.ItemAttributes, 'TradeInValue'):
                        trade_value = item.ItemAttributes.TradeInValue.Amount / 100.0
                    else:
                        trade_value = 0

                    if hasattr(item.ItemAttributes, 'LowestUsedPrice'):
                        lowest_used_price = item.OfferSummary.LowestUsedPrice.Amount / 100.0
                    else:
                        lowest_used_price = 999

                    if hasattr(item.ItemAttributes, 'LowestNewPrice'):
                        lowest_new_price = item.OfferSummary.LowestNewPrice.Amount / 100.0
                    else:
                        lowest_new_price = 999

                    price = min(lowest_used_price, lowest_new_price)
                    profit = (trade_value - price) - 3.99
                    roi = round(float(profit / price * 100), 2)

                    if not profit > 10:
                        # item_frame.drop(item_frame.loc[item_frame['asin'] == asin])
                        continue
                    else:
                        print 'Profit Found\n\tASIN - {}\n\tPrice - {}\n\tProfit{}\n\tROI - {}'.format(asin, price, profit, roi)
                        item_frame.loc[item_frame['asin'] == asin, 'trade_in_eligible'] = trade_in_eligible
                        item_frame.loc[item_frame['asin'] == asin, 'trade_value'] = trade_value
                        item_frame.loc[item_frame['asin'] == asin, 'price'] = price
                        item_frame.loc[item_frame['asin'] == asin, 'profit'] = profit
                        item_frame.loc[item_frame['asin'] == asin, 'roi'] = '${}'.format(roi)
                else:
                    # item_frame.drop(item_frame.loc[item_frame['asin'] == asin])
                    continue
            else:
                # item_frame.drop(item_frame.loc[item_frame['asin'] == asin])
                continue
    return item_frame.dropna()


def amzn_search(asins):
    api = API(locale='us')
    response = _get_amzn_response(asins, api)
    if not response:
        return None
    else:
        return response


def _get_amzn_response(asins, api):
    query = 'response = api.item_lookup(",".join(asins), ResponseGroup="Large")'
    err_count = 0
    while True:
        try:
            exec(query)
            return response

        except AWSError, e:
            err_count += 1
            print 'AWS Error: {}'.format(e.code)
            if err_count > 5:
                return None
            time.sleep(2)
            continue


def _chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))


if __name__ == '__main__':
    conn = boto.connect_s3()
    bucket = conn.get_bucket('textbook-arbitrage')
    api_cols = ['trade_in_eligible', 'trade_value', 'price', 'profit', 'roi']

    keys = bucket.list()
    latest_items_key = item_keys(keys)[0]

    frame = get_item_frame()
    price_frame = get_price_data(frame)
    upload_results(price_frame)

    print 'finished'
