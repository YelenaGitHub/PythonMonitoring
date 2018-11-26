#!/usr/bin/env python3

import os

PIPELINE_MANAGER_CONFIGURATION_NO_AVAILABLE = 'Pipeline manager configuration {} does not available.'


def get_pipeline_manager_configuration(config_name, config_path):

    pipeline_manager_template_path = os.path.join(os.path.abspath(config_path), config_name)

    if os.path.isfile(pipeline_manager_template_path):
        return pipeline_manager_template_path
    else:
        print(PIPELINE_MANAGER_CONFIGURATION_NO_AVAILABLE.format(pipeline_manager_template_path))
