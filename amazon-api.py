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

def item_keys(keys):
    regex = re.compile(r'scraping_items\/items-(.+)\.csv')
    keys = [(key, datetime.strptime(regex.search(key.name).group(1), '%m-%d-%Y')) for key in keys if regex.match(key.name)]
    latest_key = max(keys, key=itemgetter(1))[0]
    return latest_key


def upload_results(frame):
    global search_date
    if len(frame) == 0:
        return None
    k = Key(bucket)
    search_date = latest_items_key.name[latest_items_key.name.index('items-') + len('items-'):]
    k.key = '/api_results/results-{}'.format(search_date)
    k.set_contents_from_string(frame.to_csv())
    send_mail_via_smtp()
    os.remove('results.csv')


def get_item_frame():
    frame = pd.DataFrame.from_csv(latest_items_key)
    for col in api_cols:
        frame[col] = np.nan
    return frame.reset_index()


def get_price_data(item_frame):
    global search_date, items_total, item_count
    for chunk in _chunker(item_frame, 10):
        isbn10s = [row['isbn10'] for i, row in chunk.iterrows()]
        response = amzn_search(isbn10s)
        for item in response.Items.Item:
            item_count += 1
            asin = item.ASIN
            if hasattr(item.ItemAttributes, 'IsEligibleForTradeIn'):
                trade_in_eligible = bool(item.ItemAttributes.IsEligibleForTradeIn)

                if trade_in_eligible:
                    if hasattr(item.ItemAttributes, 'TradeInValue'):
                        trade_value = item.ItemAttributes.TradeInValue.Amount / 100.0
                    else:
                        trade_value = 0

                    if hasattr(item.OfferSummary, 'LowestUsedPrice'):
                        lowest_used_price = item.OfferSummary.LowestUsedPrice.Amount / 100.0
                    else:
                        lowest_used_price = 999

                    if hasattr(item.OfferSummary, 'LowestNewPrice'):
                        lowest_new_price = item.OfferSummary.LowestNewPrice.Amount / 100.0
                    else:
                        lowest_new_price = 999

                    if hasattr(item, 'DetailPageURL'):
                        url = item.DetailPageURL
                    else:
                        url = ''

                    price = min(lowest_used_price, lowest_new_price)
                    profit = (trade_value - price) - 3.99
                    roi = round(float(profit / price * 100), 2)

                    if not profit > 10:
                        print '{}/{} Not Profitable - {}'.format(item_count, items_total, asin)
                        continue
                    else:
                        print '{}/{} Profit Found\n\tisbn10 - {}\n\tPrice - {}\n\tProfit - {}\n\tROI - {}'.format(item_count, items_total, asin, price, profit, roi)
                        item_frame.loc[item_frame['isbn10'] == asin, 'trade_in_eligible'] = trade_in_eligible
                        item_frame.loc[item_frame['isbn10'] == asin, 'trade_value'] = trade_value
                        item_frame.loc[item_frame['isbn10'] == asin, 'price'] = price
                        item_frame.loc[item_frame['isbn10'] == asin, 'profit'] = profit
                        item_frame.loc[item_frame['isbn10'] == asin, 'roi'] = '${}'.format(roi)
                        item_frame.loc[item_frame['isbn10'] == asin, 'url'] = url
                else:
                    # item_frame.drop(item_frame.loc[item_frame['isbn10'] == isbn10])
                    continue
            else:
                # item_frame.drop(item_frame.loc[item_frame['isbn10'] == isbn10])
                continue

    result_frame = item_frame.dropna()
    result_frame.to_csv('results.csv'.format(search_date))
    return result_frame


def amzn_search(isbn10s):
    api = API(locale='us')
    response = _get_amzn_response(isbn10s, api)
    if not response:
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
            # print 'AWS Error: {}'.format(e.code)
            try:
                if e[1] in isbn10s:
                    isbn10s.remove(e[1])
                    items_total -= 1
                    # print '\t{} Dropped - Not asin'.format(e[1])
            except:
                pass
            if err_count > 10:
                return None
            time.sleep(2)
            continue


def _chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))


def send_mail_via_smtp():
    global search_date
    username = os.environ['YAHOO_USERNAME'] + '@yahoo.com'
    password = os.environ['YAHOO_PASSWORD']

    recipients_emails = 'kylebonnet@gmail.com'

    body = 'GET SOME'

    msg = MIMEMultipart(
        From=username,
        To=recipients_emails,
        Subject='Textbook Arbitrage Results - {}'.format(search_date)
    )

    msg.attach(MIMEText(body))

    msg.attach(MIMEApplication(
        open('results.csv'.format(search_date)).read(),
        Content_Disposition='attachment; filename=results - {}'.format(search_date),
        Name='results - {}'.format(search_date)
    ))

    try:
        smtpserver = smtplib.SMTP("smtp.mail.yahoo.com", 587)
        smtpserver.ehlo()
        smtpserver.starttls()
        smtpserver.ehlo()
        smtpserver.login(username, password)
        fromaddr = username
        smtpserver.sendmail(fromaddr, recipients_emails, msg.as_string())
        print '{0} EMAIL SENT {0}'.format('*' * 10)
    except Exception as e:
        print "failed to send mail"
        print e


if __name__ == '__main__':
    conn = boto.connect_s3(os.environ['AWS_ACCESS_KEY'], os.environ['AWS_SECRET_KEY'])
    bucket = conn.get_bucket('textbook-arbitrage')
    api_cols = ['trade_in_eligible', 'trade_value', 'price', 'profit', 'roi', 'url']
    search_date = ''
    keys = bucket.list()
    latest_items_key = item_keys(keys)

    frame = get_item_frame()
    item_count = 0
    items_total = len(frame)
    price_frame = get_price_data(frame)
    upload_results(price_frame)


    print 'finished'
