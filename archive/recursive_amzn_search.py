import glob
import os
import re
import sqlite3
import time
import urllib2
from datetime import datetime, timedelta
from operator import itemgetter

import boto
import numpy as np
import pandas as pd
from amazonproduct.api import API
from amazonproduct import AWSError

from amazon.api import AmazonAPI

def get_latest_key(keys):
    """
    gets the latest key from s3 bucket "textbook arbitrage"
    :param keys:
    :return:
    """
    regex = re.compile(r'scraping_items\/items-(.+)\.csv')
    keys = [(key, datetime.strptime(regex.search(key.name).group(1), '%m-%d-%Y')) for key in keys if regex.match(key.name)]
    latest_key = max(keys, key=itemgetter(1))[0]
    return latest_key


def write_item_key(key_file):
    """
    writes item file to S3 bucket
    :param key_file: item file in LOCAL OUTPUT DIR
    :return:
    """
    path = r'scraping_items/'
    full_key_name = os.path.join(path, os.path.basename(key_file))
    k = bucket.new_key(full_key_name)
    k.set_contents_from_filename(key_file)


def write(text, fname, profit=False):
    """
    writes s to file fname
    :param text:
    :param fname:
    :param profit:
    :return:
    """
    with open(fname, 'a') as f:
        f.write(text)
        if not profit:
            f.write('\n')


def recursive_amzn(asin, depth=3):
    """
    recursive api calls for items similar to asin
    :param asin:
    :param depth:
    :return:
    """
    global tab_depth, log_file
    depth -= 1
    tab_depth = depth
    if depth > 0:

        response = amzn_search(asin)
        # response = simple_amzn_search(asin)

        if response is not None:
            try:
                found = [item for item in response.Items.Item if not seendb(item.ASIN.text)]
                trade_eligible_found = [item for item in found if trade_eligible(item)]

                # not sure if should record items ineligible for trade
                # not_trade_eligible = [item for item in found if item not in trade_eligible_found]  # still record items for future searching
                # for item in not_trade_eligible:
                #     if hasattr(item, 'ASIN'):
                #         write('{}, False'.format(item.ASIN), item_file)

                for item in trade_eligible_found:
                    yield item
                    for nitem in recursive_amzn(item.ASIN.text, depth):
                        yield nitem

                yield None
            except AttributeError:
                yield None
        else:
            yield None


def simple_amzn_search(asin):
    response = amazon.similarity_lookup(ItemId=asin)
    if len(response) == 0:
        response = None
    return response


def amzn_search(asin):
    """
    amazon api call for similar items. If initial fails, tries item_lookup instead
    :param asin: amazon id number
    :return:
    """
    response = None
    throttle = timedelta(seconds=1/api.REQUESTS_PER_SECOND)
    t1 = datetime.now()
    try:  # try similar search
        response = api.similarity_lookup(asin, ResponseGroup='Large')
    except AWSError as e:
        # nothing similar, throttle and try item_lookup
        if hasattr(e, 'code') and e.code == 'AWS.ECommerceService.NoSimilarities':
            write('No Similar - {}'.format(asin), log_file)
            t2 = datetime.now()
            wait = throttle - (t2 - t1)
            time.sleep(wait.seconds + wait.microseconds / 1000000.0)
            try:  # no similar items, look up asin instead
                response = api.item_lookup(asin, ResponseGroup='Large')
            except AWSError as e:
                print 'amzn_search Unhandled AWSError'
                write('\tERROR amzn_search {} - UNHANDLED AWS: {}'.format(datetime.now(), e.code), log_file)
        # request throttled, wait and try again
        elif hasattr(e, 'message') and e.message == 'timed out':
            print 'amzn_search timed out'
            time.sleep(api.REQUESTS_PER_SECOND)
            amzn_search(asin)
        # unhandled exception
        elif hasattr(e, 'errno') and e.errno == 10054:
            print 'amzn_search Unhandled Exception'
            write('ERROR amzn_search {} - UNHANDLED EXCEPTION: {}'.format(datetime.now(), e), log_file)
            time.sleep(api.REQUESTS_PER_SECOND)
            amzn_search(asin)
        else:
            print e

    return response


def trade_eligible(item):
    """
    checks item for trade eligibility
    :param item:
    :return: boolean
    """
    if hasattr(item.ItemAttributes, 'IsEligibleForTradeIn'):
        return True
    else:
        return False


def check_profit(items):
    """
    checks items that are trade eligible for a profit on trade in. Looks at purchase price less shipping
    and compares this to the trade value to establish profit. If a profit is found, item is written to output
    :param items: current list of api response item(s) (up to 10)
    :return:
    """
    global profit_count, count, tab_depth, max_depth
    for item in items:
        if item is None:
            continue
        count += 1
        write('{}{} - {}'.format('\t' * (max_depth - tab_depth), count, item.ASIN), log_file)
        write('{},{}'.format(item.ASIN.text, 'True'), item_file)

        trade_value, lowest_used_price, lowest_new_price, url = check_price_attributes(item)

        if trade_value:
            price = min(lowest_used_price, lowest_new_price)
            profit = (trade_value - price) - 3.99  # discount profit to include shipping
            roi = round(float(profit / price * 100), 2)

            if profit >= profit_min and roi >= roi_min:
                profit_count += 1
                print '{} - Profit of {} found - {}'.format(count, profit, item.ASIN)
                write('{0}, {1}, {2}, {3}, {4}\n'.format(item.ASIN, price, profit, roi, url), fname=profitable_file, profit=True)
        else:
            continue


def check_price_attributes(item):
    trade_value, lowest_used_price, lowest_new_price, url = (None for i in range(4))
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
    return trade_value, lowest_used_price, lowest_new_price, url


def seendb(asin):
    """
    checks if asin has been seen
    :param asin: amazon product id
    :return: boolean
    """
    cur.execute('SELECT * FROM seen WHERE ID=?', [asin])
    if cur.fetchone():
        # Item already in database
        return True

    cur.execute('INSERT INTO seen VALUES(?)', [asin])
    sql.commit()

    return False


def main(asin_key, max_depth):
    """
    main execution. takes latest S3 asin key and begins the recursive search.
    :param asin_key: latest asin key in S3 bucket
    :param max_depth: max recursion depth to be performed
    :return:
    """
    global count
    # create download url for key file
    response = urllib2.urlopen(asin_key.generate_url(120))  # download url expires in 120 sec

    asin_frame = pd.read_csv(response)

    # randomize frame and sort by trade_eligible = True
    asin_frame = asin_frame.reindex(np.random.permutation(asin_frame.index)).sort('trade_eligible', ascending=False).reset_index(drop=True)
    asin_frame.index += 1
    for row in asin_frame[1:].iterrows():
        row = row[1]
        count += 1
        asin = row.isbn10
        write('{} - {}'.format(count, asin), log_file)
        write('{},{}'.format(asin, 'True'), item_file)
        next_asin_set = recursive_amzn(asin, depth=max_depth)

        # try:
        check_profit(next_asin_set)
        # except Exception as e:
        #     print 'main exception', e
        #     write('ERROR main - {}'.format(e), log_file)
        #     continue


if __name__ == '__main__':
    # boto connection
    AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')
    AWS_SECRET_KEY = os.environ.get('AWS_SECRET_KEY')
    conn = boto.connect_s3(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    bucket = conn.get_bucket('textbook-arbitrage')

    # key parameters
    date = datetime.today().date().strftime('%m-%d-%Y')
    search_date = ''

    # get key
    keys = bucket.list()
    latest_items_key = get_latest_key(keys)

    # set up amazon-product-api
    api = API(locale='us')
    # amazon = AmazonAPI(AWS_ACCESS_KEY, AWS_SECRET_KEY, 'boutiqueguita-20')

    # misc variables
    profit_min = 10
    roi_min = 15
    count = 0
    profit_count = 0
    items = []
    max_depth = 3  # set depth to check similar items
    tab_depth = 1

    # set up output location
    LOCAL_OUTPUT_DIR = os.path.join(os.environ.get('HOME'), 'Desktop', 'Recursive Search Results')
    # LOCAL_OUTPUT_DIR = os.path.join(os.environ.get('ONEDRIVE_PATH'), 'Recursive Search Results')
    log_file = os.path.join(LOCAL_OUTPUT_DIR, 'Logs', 'log - {}.csv'.format(date))
    profitable_file = os.path.join(LOCAL_OUTPUT_DIR, 'Profitable', 'profitable - {}.csv'.format(date))
    item_file = os.path.join(LOCAL_OUTPUT_DIR, 'Items', 'items-{}.csv'.format(date))

    # crete out dirs if not there
    if not os.path.isdir(os.path.join(LOCAL_OUTPUT_DIR, 'Items')): os.makedirs(os.path.join(LOCAL_OUTPUT_DIR, 'Items'))
    if not os.path.isdir(os.path.join(LOCAL_OUTPUT_DIR, 'Logs')): os.makedirs(os.path.join(LOCAL_OUTPUT_DIR, 'Logs'))
    if not os.path.isdir(os.path.join(LOCAL_OUTPUT_DIR, 'Profitable')): os.makedirs(os.path.join(LOCAL_OUTPUT_DIR, 'Profitable'))

    # create or overwrite out files
    open(log_file, 'wb').close()
    with open(profitable_file, 'wb') as f:
        f.write('{}, {}, {}, {}, {}\n'.format('asin', 'price', 'profit', 'roi', 'url'))
    with open(item_file, 'wb') as f:
        f.write('{},{}\n'.format('isbn10', 'trade_eligible'))

    # seen db
    db_dir = os.path.join(LOCAL_OUTPUT_DIR, 'Items')
    # remove last db if it's there
    try:
        os.remove(max(glob.glob(os.path.join(db_dir, '*.db')), key=os.path.getmtime))
    except:
        pass
    dup_db = os.path.join(db_dir, 'dup - {}.db'.format(date))
    sql = sqlite3.connect(dup_db)
    cur = sql.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS seen(id TEXT)')

    # execution
    start = time.time()
    print '**** SCRIPT STARTED AT {} ****'.format(time.ctime(int(time.time())))
    # try:
    main(latest_items_key, max_depth)
    # except Exception as e:
    #     print '****ERROR IN MAIN EXECUTION****'
    #     print e
    end = time.time()


    print '*' * 15
    print '**** SCRIPT ENDED AT {} ****'.format(time.ctime(int(time.time())))
    print '**** SCRIPT EXECUTION TIME - {} hrs ****'.format(round((end - start)/3600, 2))
    print '**** SCRIPT EXECUTION TIME - {} mins ****'.format(round((end - start)/60, 2))
    print '**** SCRIPT PERFORMANCE - {} ITEMS/min ****'.format(round(count / ((end - start)/60), 2))
    print '**** SCRIPT PERFORMANCE - {} PROFITABLE/min ****'.format(round(profit_count / ((end - start)/60), 2))
    print '**** {} PROFITABLE BOOKS IDENTIFIED ****'.format(profit_count)


    # closeout
    cur.close()
    if count > 1000:
        write_item_key(item_file)
