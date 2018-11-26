#!/usr/bin/env python3

import pandas as pd

pd.set_option('display.max_colwidth', -1)


def create_html_table(data_list, column_list, title_list):

    df_report = pd.DataFrame(data = data_list, columns = column_list)
    df_report = df_report.fillna('-')
    df_report.columns = title_list

    return df_report.to_html(index = False, border = 0, justify = 'left')