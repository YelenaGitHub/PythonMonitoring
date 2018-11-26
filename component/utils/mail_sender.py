#!/usr/bin/env python3

import os
import smtplib
import socket
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

SMTP_HOST = 'localhost'
SMTP_PORT = 25

SENDER_NAME = 'monitoring'
SENDER_HOST = socket.getfqdn()
SENDER = SENDER_NAME + '@' + SENDER_HOST

EMAIL_TEXT_PLAIN = 'plain'
EMAIL_TEXT_HTML = 'html'

ENVIRONMENT_TYPE = os.environ['SYSTEM_ENVIRONMENT']

# email lists are separated by \n symbols
RECIPIENTS_LIST = os.environ['RECIPIENTS_LIST']

EMAIL_FAILED_MESSAGE = "Email has not sent..."


def send_email(subject_name, message_body, isHtml):
    try:
        recipients = RECIPIENTS_LIST.split()

        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        if isHtml:
            msg = MIMEMultipart('alternative')
            html_body = get_html_body(message_body)

            plainMessage = MIMEText(message_body, EMAIL_TEXT_PLAIN)
            htmlMessage = MIMEText(html_body, EMAIL_TEXT_HTML)

            msg.attach(plainMessage)
            msg.attach(htmlMessage)
        else:
            msg = MIMEText(message_body)

        msg['Subject'] = ENVIRONMENT_TYPE + ' ' + subject_name
        msg['From'] = SENDER
        msg['To'] = ", ".join(recipients)

        #### fixme only for testing
        #server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        #server.ehlo()
        #server.login('***', '***')
        ### end of test

        server.sendmail(SENDER, recipients, msg.as_string())
    except Exception as e:
        sys.exit(EMAIL_FAILED_MESSAGE + str(e))
    finally:
        server.quit()


def get_html_body(messageText):

    html_body = """\
    <html>
      <head></head>
      <body>
        {}
      </body>
    </html>
    """.format(messageText)

    return html_body

