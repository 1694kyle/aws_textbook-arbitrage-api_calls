from amazonproduct.api import API
from datetime import datetime
import os

def write(text, fname):
    with open(fname, 'a') as f:
        f.write(text)
        f.write('\n')


def similar_items(asin):
    response = api.similarity_lookup(asin, ResponseGroup='Large')
    return response


def main():
    global count, items

    response = api.browse_node_lookup(465600)

    for child in response.BrowseNodes.BrowseNode.Children.BrowseNode:
        write('{} ({})'.format(child.Name, child.BrowseNodeId), fname=log_file)
        r = api.item_search(search_index='Books', BrowseNode=child.BrowseNodeId, ResponseGroup='Large')
        page_count = 0
        for page in r.iterpages():
            page_count += 1
            write('*****PAGE {}*****'.format(page_count), fname=log_file)
            for item in page.Items.Item:
                count += 1
                write('\t{} - {}'.format(count, item.ASIN), fname=log_file)  # write to log file

                similar = similar_items(item.ASIN.text)
                for similar_item in similar.Items.Item:
                    if similar_item.ASIN.text not in items and trade_eligible(similar_item):
                        count += 1
                        write('\t\t{} - {}'.format(count, similar_item.ASIN), fname=log_file)  # write to log file
                        write('{}'.format(similar_item.ASIN), fname=item_file)  # write to item file
                        items.append(similar_item.ASIN.text)
                    else:
                        continue

                if item.ASIN.text not in items and trade_eligible(item):
                    write('{}'.format(item.ASIN), fname=item_file)  # write to item file
                    items.append(item.ASIN.text)

        print 'Finished {}'.format(child.Name)


def trade_eligible(item):
    if hasattr(item.ItemAttributes, 'IsEligibleForTradeIn'):
        return item
    else:
        return None


def check_profit(item):
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

    if profit > 5:
        print '\tProfit of {} found - {}'.format(profit, item.ASIN)
        write('{0}, {1}, {2}, {3}, {4}\n'.format(item.ASIN, price, profit, roi, url), fname=profitable_file)


if __name__ == '__main__':

    api = API(locale='us')
    count = 0

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

    main()
