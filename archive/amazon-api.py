from amazonproduct.api import API
from amazonproduct.errors import AWSError
import boto
from boto.s3.key import Key
from datetime import datetime
import re
import pandas as pd
import numpy as np
import time
import os
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from operator import itemgetter
import time

from result_email import send_mail_via_smtp


def item_keys(keys):
    regex = re.compile(r'scraping_items\/items-(.+)\.csv')
    keys = [(key, datetime.strptime(regex.search(key.name).group(1), '%m-%d-%Y')) for key in keys if regex.match(key.name)]
    latest_key = max(keys, key=itemgetter(1))[0]
    return latest_key


def upload_frame(frame, type):
    global search_date
    k = Key(bucket)
    search_date = latest_items_key.name[latest_items_key.name.index('items-') + len('items-'):]
    if type == 'result':
        if len(frame) == 0:
            return None
        k.key = '/api_results/results-{}'.format(search_date)
        k.set_contents_from_string(frame.to_csv(index=False))
        send_mail_via_smtp(pro)
        os.remove('results.csv')
    else:
        k.key = '/scraping_items/items-{}'.format(search_date)
        k.set_contents_from_string(frame.to_csv(columns=['isbn10', 'trade_eligible'], index=False))


def get_item_frame():
    temp_frame = pd.read_csv(latest_items_key, iterator=True, chunksize=1000)
    frame = pd.concat(temp_frame, ignore_index=True)
    try:
        frame = frame.sort('trade_eligible', ascending=False)
    except KeyError:
        pass
    for col in api_cols:
        frame[col] = np.nan

    return frame.reset_index()


def get_price_data(item_frame):
    global search_date, items_total, item_count, profitable_item_count
    for chunk in _chunker(item_frame, 10):
        isbn10s = [row['isbn10'] for i, row in chunk.iterrows()]
        try:
            response = amzn_search(isbn10s)
        except:
            continue

        if response is None:
            continue

        for item in response.Items.Item:

            if item is None:
                continue

            item_count += 1
            asin = item.ASIN
            if hasattr(item.ItemAttributes, 'IsEligibleForTradeIn'):
                try:
                    trade_in_eligible = bool(item.ItemAttributes.IsEligibleForTradeIn)
                except:
                    trade_in_eligible = False

                if trade_in_eligible:
                    if hasattr(item.ItemAttributes, 'TradeInValue'):
                        try:
                            trade_value = item.ItemAttributes.TradeInValue.Amount / 100.0
                        except:
                            trade_value = 0
                    else:
                        trade_value = 0

                    if hasattr(item.OfferSummary, 'LowestUsedPrice'):
                        try:
                            lowest_used_price = item.OfferSummary.LowestUsedPrice.Amount / 100.0
                        except:
                            lowest_used_price = 999
                    else:
                        lowest_used_price = 999

                    if hasattr(item.OfferSummary, 'LowestNewPrice'):
                        try:
                            lowest_new_price = item.OfferSummary.LowestNewPrice.Amount / 100.0
                        except:
                            lowest_new_price = 999
                    else:
                        lowest_new_price = 999

                    if hasattr(item, 'DetailPageURL'):
                        try:
                            url = item.DetailPageURL
                        except:
                            url = ''
                    else:
                        url = ''

                    price = min(lowest_used_price, lowest_new_price)
                    profit = (trade_value - price) - 3.99
                    roi = round(float(profit / price * 100), 2)

                    if not profit > 10:
                        write(LOCAL_LOG_FILE, '{}/{} Not Profitable - {}'.format(item_count, items_total, asin))
                        # print '{}/{} Not Profitable - {}'.format(item_count, items_total, asin)
                        continue
                    else:
                        write(LOCAL_LOG_FILE, '{}/{} Profit Found\n\tisbn10 - {}\n\tPrice - {}\n\tProfit - {}\n\tROI - {}'.format(item_count, items_total, asin, price, profit, roi))
                        write(LOCAL_OUTPUT_FILE, '{},{},{},{},{}'.format(asin, price, profit, roi, url))
                        print '{}/{} Profit Found\n\tisbn10 - {}\n\tPrice - {}\n\tProfit - {}\n\tROI - {}'.format(item_count, items_total, asin, price, profit, roi)
                        item_frame.loc[item_frame['isbn10'] == asin, 'trade_in_eligible'] = trade_in_eligible
                        item_frame.loc[item_frame['isbn10'] == asin, 'trade_value'] = trade_value
                        item_frame.loc[item_frame['isbn10'] == asin, 'price'] = price
                        item_frame.loc[item_frame['isbn10'] == asin, 'profit'] = profit
                        item_frame.loc[item_frame['isbn10'] == asin, 'roi'] = '${}'.format(roi)
                        item_frame.loc[item_frame['isbn10'] == asin, 'url'] = url
                        profitable_item_count += 1
                else:
                    write(LOCAL_LOG_FILE, '{}/{} Not Trade Eligible - {}'.format(item_count, items_total, asin))
                    # print '{}/{} Not Trade Eligible - {}'.format(item_count, items_total, asin)
                    trade_in_eligible = False

            else:
                write(LOCAL_LOG_FILE, '{}/{} Not Trade Eligible - {}'.format(item_count, items_total, asin))
                # print '{}/{} Not Trade Eligible - {}'.format(item_count, items_total, asin)
                trade_in_eligible = False

            if trade_in_eligible is False:
                write(LOCAL_DROPPED_FILE, asin.text)
                frame.drop(frame.loc[frame['isbn10'] == asin].index[0], inplace=True)


    result_frame = item_frame.dropna()
    result_frame.to_csv('results.csv'.format(search_date))
    return result_frame


def amzn_search(isbn10s):
    api = API(locale='us')
    response = _get_amzn_response(isbn10s, api)
    if response is None:
        return None
    else:
        return response


def _get_amzn_response(isbn10s, api):
    global items_total
    query = 'response = api.item_lookup(",".join(isbn10s), ResponseGroup="Large")'
    err_count = 0
    while True:
        try:
            exec(query)
            return response
        except AWSError, e:
            err_count += 1
            try:
                if e[1] in isbn10s:
                    # write(LOCAL_DROPPED_FILE, e[1])

                    isbn10s.remove(e[1])
                    items_total -= 1
            except:
                pass
            if err_count > 10:
                return None
            time.sleep(2)
            continue


def _chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))


def write(file_path, text):
    with open(file_path, 'a') as f:
        f.write(text)
        f.write('\n')





if __name__ == '__main__':

    conn = boto.connect_s3(os.environ['AWS_ACCESS_KEY'], os.environ['AWS_SECRET_KEY'])
    bucket = conn.get_bucket('textbook-arbitrage')
    api_cols = ['trade_value', 'price', 'profit', 'roi', 'url']
    date = datetime.today().date().strftime('%m-%d-%Y')
    search_date = ''
    keys = bucket.list()
    latest_items_key = item_keys(keys)

    LOCAL_OUTPUT_DIR = os.path.join(os.environ.get('HOME'), 'Desktop', 'Scraping Results')
    LOCAL_OUTPUT_FILE = os.path.join(LOCAL_OUTPUT_DIR, 'Results', 'results {}'.format(date))
    LOCAL_LOG_FILE = os.path.join(LOCAL_OUTPUT_DIR, 'Logs', 'log {}'.format(date))
    LOCAL_DROPPED_FILE = os.path.join(LOCAL_OUTPUT_DIR, 'Dropped', 'dropped_items {}'.format(date))
    if not os.path.isdir(os.path.join(LOCAL_OUTPUT_DIR, 'Results')): os.makedirs(os.path.join(LOCAL_OUTPUT_DIR, 'Results'))
    if not os.path.isdir(os.path.join(LOCAL_OUTPUT_DIR, 'Logs')): os.makedirs(os.path.join(LOCAL_OUTPUT_DIR, 'Logs'))
    if not os.path.isdir(os.path.join(LOCAL_OUTPUT_DIR, 'Dropped')): os.makedirs(os.path.join(LOCAL_OUTPUT_DIR, 'Dropped'))

    with open(LOCAL_OUTPUT_FILE,'wb') as f:
        for header in ['asin,', 'price,', 'profit,', 'roi,', 'url\n']:
            f.write(header)
    open(LOCAL_LOG_FILE, 'wb').close()


    frame = get_item_frame()  # todo: make generator and write modifications in-place. or if not profitable, drop from this round?
    item_count = 0
    profitable_item_count = 0
    items_total = len(frame)
    now = time.time()
    price_frame = get_price_data(frame)
    diff = time.time() - now

    write(LOCAL_LOG_FILE, 'Finished {} Items:\n\t{} Hours\n\t{} Minutes\n\t{} Items/sec\n\t{} Profitable'.format(item_count, round(diff/3600, 2), round(diff/60, 2), round(item_count/diff, 2), profitable_item_count))
    print 'Finished {} Items:\n\t{} Hours\n\t{} Minutes\n\t{} Items/sec\n\t{} Profitable'.format(item_count, round(diff/3600, 2), round(diff/60, 2), round(item_count/diff, 2), profitable_item_count)

    upload_frame(price_frame, type='result')  # upload results if any

    upload_frame(frame, type='update') # overwrite existing items with trimmed down list asins

    write(LOCAL_LOG_FILE, 'finished')
    print 'finished'

    if os.stat(LOCAL_OUTPUT_FILE).st_size == 0:
        os.remove(LOCAL_OUTPUT_FILE)