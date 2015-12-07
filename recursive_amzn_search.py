from amazonproduct.api import API
from datetime import datetime
import os
from result_email import send_mail_via_smtp
from amazonproduct.errors import AWSError
from operator import itemgetter
import re
import boto
import csv
import urllib2
import operator
import time
import sqlite3


def item_keys(keys):
    regex = re.compile(r'scraping_items\/items-(.+)\.csv')
    keys = [(key, datetime.strptime(regex.search(key.name).group(1), '%m-%d-%Y')) for key in keys if regex.match(key.name)]
    latest_key = max(keys, key=itemgetter(1))[0]
    return latest_key


def write(text, fname, profit=False):
    with open(fname, 'a') as f:
        f.write(text)
        if not profit:
            f.write('\n')


def recursive_amzn(asin, depth=3):
    global tab_depth
    depth -= 1
    tab_depth = depth
    if depth > 0:
        try:
            response = api.similarity_lookup(asin, ResponseGroup='Large')
        except:
            try:
                response = api.item_lookup(asin, ResponseGroup='Large')
            except:
                response = None
        if response is not None:
            found = [item for item in response.Items.Item if not seendb(item.ASIN.text)]
            trade_eligible_found = [item for item in found if trade_eligible(item)]
            for item in trade_eligible_found:
                yield item
                for nitem in recursive_amzn(item.ASIN.text, depth):
                    yield nitem
        else:
            yield None


def trade_eligible(item):
    if hasattr(item.ItemAttributes, 'IsEligibleForTradeIn'):
        return True
    else:
        return False


def check_profit(items):
    global profit_count, count, tab_depth, max_depth
    for item in items:
        if item is None:
            continue
        write('{}{} - {}'.format('\t' * (max_depth - tab_depth), count, item.ASIN), log_file)
        count += 1
        if hasattr(item.ItemAttributes, 'TradeInValue'):
            try:
                trade_value = item.ItemAttributes.TradeInValue.Amount / 100.0
            except:
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
            # print '{}\n\tPrice: {}\n\tProfit: {}\n\tROI: {}'.format(item.ASIN, price, profit, roi)
            # write(fname=log_file, text='\tPrice: {}\n\tProfit: {}\n\tROI: {}'.format(price, profit, roi))

            if profit > 5:
                profit_count += 1
                print '{} - Profit of {} found - {}'.format(count, profit, item.ASIN)
                write('{0}, {1}, {2}, {3}, {4}\n'.format(item.ASIN, price, profit, roi, url), fname=profitable_file, profit=True)
        else:
            continue


def seendb(asin):
    cur.execute('SELECT * FROM seen WHERE ID=?', [asin])
    if cur.fetchone():
        # Post is already in the database
        return True

    cur.execute('INSERT INTO seen VALUES(?)', [asin])
    sql.commit()

    return False


def main(asin_key, max_depth):
    global count, items
    # create download url for key file
    response = urllib2.urlopen(asin_key.generate_url(5))
    asin_csv = csv.reader(response)
    asin_csv.next()  # skip header row
    asin_csv = sorted(asin_csv, key=operator.itemgetter(1), reverse=True)  # sort on trade eligible books
    for row in asin_csv:
        count += 1
        asin = row[0]
        write('{} - {}'.format(count, asin), log_file)
        next_asin_set = recursive_amzn(asin, depth=max_depth)

        try:
            check_profit(next_asin_set)
        except Exception as e:
            print e
            continue


if __name__ == '__main__':
    # boto connection
    conn = boto.connect_s3(os.environ['AWS_ACCESS_KEY'], os.environ['AWS_SECRET_KEY'])
    bucket = conn.get_bucket('textbook-arbitrage')

    # key parameters
    date = datetime.today().date().strftime('%m-%d-%Y')
    search_date = ''

    # get key
    keys = bucket.list()
    latest_items_key = item_keys(keys)

    # set up api
    api = API(locale='us')

    # misc variables
    count = 0
    profit_count = 0
    date = datetime.today().date()
    items = []
    max_depth = 3
    tab_depth = 1

    # set up output location
    LOCAL_OUTPUT_DIR = os.path.join(os.environ.get('HOME'), 'Desktop', 'Recursive Search Results')
    log_file = os.path.join(LOCAL_OUTPUT_DIR, 'Logs', 'log - {}.csv'.format(date))
    profitable_file = os.path.join(LOCAL_OUTPUT_DIR, 'Profitable', 'profitable - {}.csv'.format(date))

    if not os.path.isdir(os.path.join(LOCAL_OUTPUT_DIR, 'Items')): os.makedirs(os.path.join(LOCAL_OUTPUT_DIR, 'Items'))
    if not os.path.isdir(os.path.join(LOCAL_OUTPUT_DIR, 'Logs')): os.makedirs(os.path.join(LOCAL_OUTPUT_DIR, 'Logs'))
    if not os.path.isdir(os.path.join(LOCAL_OUTPUT_DIR, 'Profitable')): os.makedirs(os.path.join(LOCAL_OUTPUT_DIR, 'Profitable'))

    open(log_file, 'wb').close()
    with open(profitable_file, 'wb') as f:
        f.write('{}, {}, {}, {}, {}\n'.format('asin', 'price', 'profit', 'roi', 'url'))

    # seen db
    dup_db = os.path.join(LOCAL_OUTPUT_DIR, 'Items', 'dup - {}.db'.format(date))
    sql = sqlite3.connect(dup_db)
    cur = sql.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS seen(id TEXT)')

    # execution
    start = time.time()
    print '**** SCRIPT STARTED AT {} ****'.format(time.ctime(int(time.time())))
    try:
        main(latest_items_key, max_depth)
    except Exception as e:
        print '****ERROR IN MAIN EXECUTION****'
        print e
    end = time.time()
    print '*' * 15
    print '**** SCRIPT ENDED AT {} ****'.format(time.ctime(int(time.time())))
    print '**** SCRIPT EXECUTION TIME - {} HRS ****'.format(round((end - start)/3600, 2))
    print '**** {} PROFITABLE BOOKS IDENTIFIED ****'.format(profit_count)

    # closeout
    cur.close()
    if os.path.isfile(dup_db):
        os.remove(dup_db)  # delete duplicate db
    if profit_count > 0:  # send email if profitable items
        send_mail_via_smtp(profitable_file)
    else:
        os.remove(profitable_file)

