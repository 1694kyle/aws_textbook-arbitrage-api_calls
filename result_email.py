
import os
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from datetime import datetime

def send_mail_via_smtp(attachment):
    date = datetime.today().date()
    username = os.environ['YAHOO_USERNAME'] + '@yahoo.com'
    password = os.environ['YAHOO_PASSWORD']

    recipients_emails = 'kylebonnet@gmail.com'

    body = 'GET SOME'

    msg = MIMEMultipart(
        From=username,
        To=recipients_emails,
        Subject='Textbook Arbitrage Results - {}'.format(date)
    )

    msg.attach(MIMEText(body))

    msg.attach(MIMEApplication(
        open(attachment).read(),
        Content_Disposition='attachment; filename=results - {}'.format(date),
        Name='profitable - {}'.format(date)
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
        print "failed to send email"
        print e