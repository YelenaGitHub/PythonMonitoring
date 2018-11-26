#!/usr/bin/env python3

import json

import requests
from requests.auth import HTTPBasicAuth

STREAMSET_REST_PIPELINE_STATUS = '/rest/v1/pipelines/status'

PIPELINE_STATUS_NAME = 'status'
PIPELINE_STARTING_STATUS = 'STARTING'
PIPELINE_RUNNING_STATUS = 'RUNNING'
PIPELINE_FINISHING_STATUS = 'FINISHING'
PIPELINE_FINISHED_STATUS = 'FINISHED'
PIPELINE_EDITED_STATUS = 'EDITED'
PIPELINE_STOPPED_STATUS = 'STOPPED'

PIPELINE_CHECKED_STATUS_NAME = 'checked_status'

PIPELINE_HD_POSTFIX = '_ID'
PIPELINE_HD_VALUE = 'HD'

PIPELINE_REST_PIPELINE_ID_NAME = 'pipelineId'

PIPELINE_HD_CORRECT_STATUSES = [PIPELINE_STOPPED_STATUS,
                                PIPELINE_EDITED_STATUS,
                                PIPELINE_STARTING_STATUS,
                                PIPELINE_RUNNING_STATUS,
                                PIPELINE_FINISHING_STATUS,
                                PIPELINE_FINISHED_STATUS]
PIPELINE_OTHERS_CORRECT_STATUSES = [PIPELINE_RUNNING_STATUS]

PIPELINE_MANAGER_JSON_COUNTRY = 'countries'
PIPELINE_MANAGER_JSON_NAME = 'name'
PIPELINE_MANAGER_JSON_TABLE = 'tables'

MONITORING_CORRECT_STATUS = 'CORRECT'
MONITORING_WRONG_STATUS = 'WRONG'

CONNECTION_FAILED_MESSAGE = 'Connection to {} has not established.\n'


""" Collects pipelines names and statuses via StreamSets REST API.
    Check the correctness of their statuses. """


def get_rest_pipelines(streamset_url, streamset_user, streamset_password):

    streamset_pipeline_status_url = streamset_url + STREAMSET_REST_PIPELINE_STATUS
    response = requests.get(streamset_pipeline_status_url,
                        auth = HTTPBasicAuth(streamset_user, streamset_password),
                        verify = False)
    return response


def get_pipeline_manager_pipelines(pipeline_manager_config_path):

    pipeline_list = []

    with open(pipeline_manager_config_path) as file_config:
        config = json.load(file_config)

        country_prefix = config[PIPELINE_MANAGER_JSON_COUNTRY][0][PIPELINE_MANAGER_JSON_NAME]
        table_name_list = config[PIPELINE_MANAGER_JSON_COUNTRY][0][PIPELINE_MANAGER_JSON_TABLE]

        for table_name in table_name_list:
            pipeline_name = country_prefix + '_' + table_name[PIPELINE_MANAGER_JSON_NAME]
            pipeline_list.append(pipeline_name)

    return pipeline_list


def get_common_pipelines(rest_pipelines, pipeline_manager_pipelines):

    common_pipelines = {}
    for pipeline_name in pipeline_manager_pipelines:
        if pipeline_name in rest_pipelines:
            common_pipelines[pipeline_name] = rest_pipelines[pipeline_name]

    if common_pipelines:
        return common_pipelines

    return common_pipelines


def get_checked_status_pipelines(rest_pipelines):

    """ Collects pipelines list consist of pipeline name, status and their checked status. """

    for pipeline_name in rest_pipelines:

        pipeline_status = str(rest_pipelines[pipeline_name][PIPELINE_STATUS_NAME])

        checked_status = check_pipeline_status(pipeline_name, pipeline_status)
        rest_pipelines[pipeline_name][PIPELINE_CHECKED_STATUS_NAME] = checked_status

    return rest_pipelines


def check_pipeline_status(pipeline_name, pipeline_status):

    checked_pipeline_name = pipeline_name.upper()

    if ((checked_pipeline_name.endswith(PIPELINE_HD_POSTFIX) and pipeline_status in PIPELINE_HD_CORRECT_STATUSES)) or (
            not checked_pipeline_name.endswith(
                    PIPELINE_HD_POSTFIX) and pipeline_status in PIPELINE_OTHERS_CORRECT_STATUSES):

        return MONITORING_CORRECT_STATUS
    else:
        return MONITORING_WRONG_STATUS


def get_pipelines_count(pipelines_list):

    record_count = len(pipelines_list)
    if record_count:
        return len(pipelines_list)
    else:
        return 0



