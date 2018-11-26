#!/usr/bin/env python3

import datetime
import os

from component.streamsets.rest_api_pipelines_status import get_pipeline_manager_pipelines, \
    get_common_pipelines, get_checked_status_pipelines, get_rest_pipelines
from component.utils.mail_sender import send_email
from component.utils.pipeline_manager_configuration import get_pipeline_manager_configuration
from component.utils.report_generate import create_html_table

# fixme delete after testing - it's just for load sample rest

# fixme test (replace)
STREAMSET_CONF_LIST = os.environ['STREAMSET_CONF_LIST']

STREAMSET_USER = os.environ['SYSTEM_USER_USER']
STREAMSET_PASSWORD = os.environ['SYSTEM_USER_PASSWORD']

EMAIL_SUBJECT = 'Daily Operations Check ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

STREAMSET_NO_REST_API_PIPELINES = 'No REST API pipelines available for processing.'
STREAMSET_ALL_PIPELINES_ANALYZED = 'All REST API pipelines will be analyzed.'
STREAMSET_CONNECTION_FAILED_MESSAGE = 'Connection to {} has not established.'

STREAMSET_INFO_COUNTRY_NAME = 'country'
STREAMSET_INFO_PROBLEM_LIST_NAME = 'problem_list'
STREAMSET_INFO_TOTAL_COUNT_NAME = 'total_count'
STREAMSET_INFO_MONITORED_COUNT_NAME = 'monitored_count'
STREAMSET_INFO_FAILED_COUNT_NAME = 'failed_count'
STREAMSET_INFO_MONITORED_PIPELINE_NAME = 'monitored_pipelines'
STREAMSET_INFO_FAILED_PIPELINE_NAME = 'failed_pipelines'
STREAMSET_INFO_PIPELINE_ID_NAME = 'pipelineId'
STREAMSET_INFO_PIPELINE_STATUS ='status'

STREAMSET_REPORT_GENERAL_INFO_TITLES = ['Organization', 'Total pipelines count', 'Monitored count', 'Failed count']
STREAMSET_REPORT_FAILED_PIPELINES_TITLES = ['Failed StreamSets pipeline name', 'Current status', 'Result']
STREAMSET_REPORT_ISSUES_TITLES = ['StreamSets Issues']

PIPELINE_MANAGER_CONFIG_PATH = '../../ansible/roles/pipelinemanager/templates/'
PIPELINE_MANAGER_CONFIG_NO_AVAILABLE = 'Pipeline manager configuration for {} does not available.'

PIPELINE_CHECKED_STATUS_NAME = 'checked_status'

PIPELINE_WRONG_STATUS = 'WRONG'


def process_streamsets_rest_api_pipelines_status():

    streamset_conf_list = STREAMSET_CONF_LIST.split()

    pipelines_by_country = []

    for streamset_conf in streamset_conf_list:
        streamset_conf_row = streamset_conf.split(";")

        streamset_url = streamset_conf_row[0]

        pipeline_manager_config = get_pipeline_manager_configuration(streamset_conf_row[1], PIPELINE_MANAGER_CONFIG_PATH)
        country_name = streamset_conf_row[2]

        pipeline_by_country = collect_streamsets_pipeline_info(streamset_url, STREAMSET_USER, STREAMSET_PASSWORD, pipeline_manager_config, country_name)
        pipelines_by_country.append(pipeline_by_country)

    report_streamsets(pipelines_by_country)


def report_streamsets(pipelines_by_country):

    """ Pipelines general info: pipelines counts by countries """
    report_html = ''
    column_list = [STREAMSET_INFO_COUNTRY_NAME, STREAMSET_INFO_TOTAL_COUNT_NAME, STREAMSET_INFO_MONITORED_COUNT_NAME, STREAMSET_INFO_FAILED_COUNT_NAME]

    title_list = STREAMSET_REPORT_GENERAL_INFO_TITLES
    report_pipelines_general_info = create_html_table(pipelines_by_country, column_list, title_list)

    report_html = report_pipelines_general_info + get_html_blank_line()

    # Collect pipelines in wrong statuses and other issues information
    failed_pipelines = []
    failed_issues = []

    for pipeline_by_country in pipelines_by_country:
        pipeline_keys = pipeline_by_country.keys()

        failed_pipelines = get_pipeline_column_list(pipeline_by_country, STREAMSET_INFO_FAILED_PIPELINE_NAME, pipeline_keys, failed_pipelines)
        failed_issues = get_pipeline_column_list(pipeline_by_country, STREAMSET_INFO_PROBLEM_LIST_NAME, pipeline_keys, failed_issues)

    if failed_pipelines:
        column_list = [STREAMSET_INFO_PIPELINE_ID_NAME, STREAMSET_INFO_PIPELINE_STATUS, PIPELINE_CHECKED_STATUS_NAME]
        title_list = STREAMSET_REPORT_FAILED_PIPELINES_TITLES

        report_pipelines_failed_info = create_html_table(failed_pipelines, column_list, title_list)
        report_html += report_pipelines_failed_info + get_html_blank_line()

    if failed_issues:
        report_issues = create_html_table(failed_issues, [''], STREAMSET_REPORT_ISSUES_TITLES)
        report_html += report_issues + get_html_blank_line()

    send_email(EMAIL_SUBJECT, report_html, True)


def get_pipeline_column_list(pipeline_by_country, pipeline_key, pipeline_keys, pipeline_list):

    if (pipeline_key in pipeline_keys):
        pipelines = pipeline_by_country[pipeline_key]
        if pipelines:
            for pipeline in pipelines:
                pipeline_list.append(pipeline)
    return pipeline_list


def collect_streamsets_pipeline_info(streamset_url, streamset_user, streamset_password, pipeline_manager_conf, country_name):

    report_country = {}
    report_country[STREAMSET_INFO_COUNTRY_NAME] = country_name

    rest_pipelines, problem_list = connect_streamsets(streamset_url, streamset_user, streamset_password)
    if problem_list:
        report_country[STREAMSET_INFO_PROBLEM_LIST_NAME] = problem_list
        return report_country

    common_pipelines, problem_list = process_common_pipelines(pipeline_manager_conf, rest_pipelines, country_name)
    incorrect_pipelines = get_incorrect_pipelines(common_pipelines)

    report_country[STREAMSET_INFO_TOTAL_COUNT_NAME] = len(rest_pipelines)
    report_country[STREAMSET_INFO_MONITORED_COUNT_NAME] = len(common_pipelines)
    report_country[STREAMSET_INFO_FAILED_COUNT_NAME] = len(incorrect_pipelines)
    report_country[STREAMSET_INFO_MONITORED_PIPELINE_NAME] = common_pipelines
    report_country[STREAMSET_INFO_PROBLEM_LIST_NAME] = set(problem_list)
    report_country[STREAMSET_INFO_FAILED_PIPELINE_NAME] = incorrect_pipelines

    return report_country


def connect_streamsets(streamset_url, streamset_user, streamset_password):
    rest_pipelines = []
    problem_list = []
    try:
        # fixme (comment only for test...)
        # check if they are available
        rest_pipelines = get_rest_pipelines(streamset_url, streamset_user, streamset_password)
        rest_pipelines = rest_pipelines.json()
        #with open(
        #    '/Users/Yelena/PycharmProjects/HomeProject/DiceMonitoring/resources/test/component/streamsets/rest_pipelines_status.json') as file_object:
        #   rest_pipelines = json.load(file_object)
        # end of test
    except Exception as e:
        err_msg = STREAMSET_CONNECTION_FAILED_MESSAGE.format(streamset_url) + str(e)
        problem_list.append(err_msg)
        print(err_msg)

    return rest_pipelines, problem_list


def process_common_pipelines(pipeline_manager_conf, rest_pipelines, country_name):

    problem_list = []

    if not rest_pipelines:
        problem_list.append(STREAMSET_NO_REST_API_PIPELINES)
        print(STREAMSET_NO_REST_API_PIPELINES)
        return

    checked_rest_pipelines = get_checked_status_pipelines(rest_pipelines)

    if not pipeline_manager_conf:
        err_msg = PIPELINE_MANAGER_CONFIG_NO_AVAILABLE.format(country_name)
        problem_list.append(err_msg)
        common_pipelines = checked_rest_pipelines
    else:
        pipeline_manager_pipelines = get_pipeline_manager_pipelines(pipeline_manager_conf)
        common_pipelines = get_common_pipelines(checked_rest_pipelines, pipeline_manager_pipelines)

    return common_pipelines, problem_list


def get_incorrect_pipelines(pipelines):

    incorrect_pipelines = []

    for pipeline_name in pipelines:
        if pipelines[pipeline_name][PIPELINE_CHECKED_STATUS_NAME] == PIPELINE_WRONG_STATUS:
            incorrect_pipelines.append(pipelines[pipeline_name])

    return incorrect_pipelines


def get_html_blank_line():
    return '<br/>'