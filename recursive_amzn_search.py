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


def item_keys(keys):
    regex = re.compile(r'scraping_items\/items-(.+)\.csv')
    keys = [(key, datetime.strptime(regex.search(key.name).group(1), '%m-%d-%Y')) for key in keys if regex.match(key.name)]
    latest_key = max(keys, key=itemgetter(1))[0]
    return latest_key


def write(text, fname):
    with open(fname, 'a') as f:
        f.write(text)
        f.write('\n')


def get_similar_items(asin, depth = 3):
    global seen
    # ending condition up front
    # function bounces back with nothing if called below depth 0
    write('{}{}'.format('\t' * depth, asin), log_file)
    if depth < 0:
        return []

    # pull all items similar to item from api
    try:
        similar_items = api.similarity_lookup(asin)
    except AWSError:
        return []

    # iterate over each new similar item
    for new_item in similar_items.Items.Item:
        write('{}{}'.format('\t' * depth, asin), log_file)
        # add to your results list the return from recursive call
        more = get_similar_items(new_item.ASIN.text, depth -1)

        similar_items.extend(more)

    return similar_items


def findem(n, seen=None, depth=3):
    depth -= 1
    if depth > 0:
        if seen is None:
            seen = set()
        found = set(api) - seen
        print(found, seen)
        seen |= found
        for item in found:
            yield item
            for nitem in findem(item, seen, depth):
                yield nitem


def trade_eligible(item):
    if hasattr(item.ItemAttributes, 'IsEligibleForTradeIn'):
        return item
    else:
        return None


def check_profit(items):
    global profit_count
    for item in items:
        if hasattr(item, 'ItemAttributes.TradeInValue'):
            try:
                trade_value = item.ItemAttributes.TradeInValue.Amount / 100.0
            except:
                trade_value = 0
        else:
            trade_value = 0

        if hasattr(item, 'OfferSummary.LowestUsedPrice'):
            try:
                lowest_used_price = item.OfferSummary.LowestUsedPrice.Amount / 100.0
            except:
                lowest_used_price = 999
        else:
            lowest_used_price = 999

        if hasattr(item, 'OfferSummary.LowestNewPrice'):
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

        if profit > 5:
            profit_count += 1
            print '\tProfit of {} found - {}'.format(profit, item.ASIN)
            write('{0}, {1}, {2}, {3}, {4}\n'.format(item.ASIN, price, profit, roi, url), fname=profitable_file)


def main(asin_key, max_depth):
    global count, items
    response = urllib2.urlopen(asin_key.generate_url(5))
    asin_csv = csv.reader(response)
    asin_csv.next()
    asin_csv = sorted(asin_csv, key=operator.itemgetter(1), reverse=True)
    for row in asin_csv:
        asin = row[0]
        write('{}{}'.format('\t' * depth, asin), log_file)
        next_asin_set = get_similar_items(asin, max_depth)
        check_profit(next_asin_set)


if __name__ == '__main__':

    conn = boto.connect_s3(os.environ['AWS_ACCESS_KEY'], os.environ['AWS_SECRET_KEY'])
    bucket = conn.get_bucket('textbook-arbitrage')
    api_cols = ['trade_value', 'price', 'profit', 'roi', 'url']
    date = datetime.today().date().strftime('%m-%d-%Y')
    search_date = ''
    keys = bucket.list()
    latest_items_key = item_keys(keys)

    api = API(locale='us')
    count = 0
    profit_count = 0
    seen = set()
    date = datetime.today().date()

    LOCAL_OUTPUT_DIR = os.path.join(os.environ.get('HOME'), 'Desktop', 'Traverse Results')

    item_file = os.path.join(LOCAL_OUTPUT_DIR, 'Items', 'items - {}.csv'.format(date))
    log_file = os.path.join(LOCAL_OUTPUT_DIR, 'Logs', 'log - {}.csv'.format(date))
    profitable_file = os.path.join(LOCAL_OUTPUT_DIR, 'Profitable', 'profitable - {}.csv'.format(date))

    if not os.path.isdir(os.path.join(LOCAL_OUTPUT_DIR, 'Items')): os.makedirs(os.path.join(LOCAL_OUTPUT_DIR, 'Items'))
    if not os.path.isdir(os.path.join(LOCAL_OUTPUT_DIR, 'Logs')): os.makedirs(os.path.join(LOCAL_OUTPUT_DIR, 'Logs'))
    if not os.path.isdir(os.path.join(LOCAL_OUTPUT_DIR, 'Profitable')): os.makedirs(os.path.join(LOCAL_OUTPUT_DIR, 'Profitable'))

    open(item_file, 'wb').close()
    open(log_file, 'wb').close()
    open(profitable_file, 'wb').close()

    items = []
    depth = 3

    main(latest_items_key, depth)

    if profit_count > 0:
        send_mail_via_smtp(profitable_file)


# todo: connect this with latest dump from s3 and search similar products in addition to those items






### Not searching enough items ###
# def main():
#     global count, items
#     response = api.browse_node_lookup(465600)
#     for child in response.BrowseNodes.BrowseNode.Children.BrowseNode:
#
#         write('{} ({})'.format(child.Name, child.BrowseNodeId), fname=log_file)
#         r = api.item_search(search_index='Books', BrowseNode=child.BrowseNodeId, ResponseGroup='Large')
#         page_count = 0
#         for page in r.iterpages():
#             page_count += 1
#             write('*****PAGE {}*****'.format(page_count), fname=log_file)
#             for item in page.Items.Item:
#                 count += 1
#                 write('\t{} - {}'.format(count, item.ASIN), fname=log_file)  # write to log file
#
#                 similar = similar_items(item.ASIN.text)
#                 if similar is not None:
#                     for similar_item in similar.Items.Item:
#                         if similar_item.ASIN.text not in items and trade_eligible(similar_item) is not None:
#                             count += 1
#                             write('\t\t{} - {}'.format(count, similar_item.ASIN), fname=log_file)  # write to log file
#                             write('{}'.format(similar_item.ASIN), fname=item_file)  # write to item file
#                             items.append(similar_item.ASIN.text)
#                         else:
#                             continue
#                 else:
#                     continue
#
#                 if item.ASIN.text not in items and trade_eligible(item) is not None:
#                     write('{}'.format(item.ASIN), fname=item_file)  # write to item file
#                     items.append(item.ASIN.text)
#
#         print 'Finished {}'.format(child.Name)