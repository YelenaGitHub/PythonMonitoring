#!/usr/bin/env python3

import datetime


def start_log_message(msg):
    current_date_time = datetime.datetime.now()
    print(msg % current_date_time.strftime('%Y-%m-%d %H:%M') + "\n")


def finish_log_message(msg):
    print("\n" + msg)


def separator_line():
    print("----------------------------------------")


def separator_bold_line():
    print("########################################")


def selected_log_line(message):
    separator_bold_line()
    print (message)
    separator_bold_line()