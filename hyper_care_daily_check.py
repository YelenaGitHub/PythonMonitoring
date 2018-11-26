#!/usr/bin/env python3

import os
import sys

from component.impala.hyper_care_daily_check_impala import LayersCheck
from component.streamsets.hyper_care_daily_check_streamsets import process_streamsets_rest_api_pipelines_status

sys.path.insert(0, os.getcwd())

def main():
    """ Daily check includes:

    1. Correctness of StreamSets pipeline statuses.
    2. Impala all layers check
    """

    # fixme uncomment
    process_streamsets_rest_api_pipelines_status()
    # fixme join result from all checks and send in an email
    process_impala_all_layers()


def process_impala_all_layers():
    layer_check = LayersCheck()
    layer_check.process()


main()
