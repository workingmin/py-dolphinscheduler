#!/bin/env python3
# -*- coding: utf-8 -*-

from http import HTTPStatus
import json
import os
import sys
import requests
import yaml


if __name__ == '__main__':
    server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
    user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')
    
    if len(sys.argv) < 3:
        print("Usage: {} <project-code> <process-definition-params.yaml>".format(sys.argv[0]))
        sys.exit(1)
    
    project_code =  sys.argv[1]
    with open(sys.argv[2], 'r') as f:
        yaml_data = yaml.safe_load(f)

    # XXX: Depend on other API that query task code list
    url = os.path.join(server_url, 'projects', project_code, 'task-definition', 'gen-task-codes')
    headers = {'token': user_token}
    params = {
        'genNum': 1
    }
    
    response = requests.get(url, headers=headers, params=params)
    status_code = response.status_code
    if status_code != HTTPStatus.OK:
        print(f'Request failed, status: {status_code}')
        sys.exit(1)
    
    json_data = response.json()
    success = json_data.get('success')
    failed = json_data.get('failed')
    if (not success) or failed:
        code = json_data.get('code')
        msg = json_data.get('msg')
        print(f'Query task code failed, code: {code}, msg: {msg}')
        sys.exit(1)
            
    task_code = json_data.get('data')[0]
        
    # Create process definition    
    url = os.path.join(server_url, 'projects', project_code, 'process-definition')
        
    name = yaml_data.get('name')
    description = yaml_data.get('description')
    global_params = "[]"
    locations = "[]"
    timeout = 0
    task_relation = [
        {
            'name': '',
            'preTaskCode': 0,
            'preTaskVersion': 0,
            'postTaskCode': task_code,
            'postTaskVersion': 1,
            'conditionType': 'NONE',
            'conditionParams': {},
            },
        ]
    rule_input_parameter = {
        'check_type': str(yaml_data.get('taskDefinition').get('taskParams').get('ruleInputParameter').get('check_type')),
        'comparison_type': int(yaml_data.get('taskDefinition').get('taskParams').get('ruleInputParameter').get('comparison_type')),
        'failure_strategy': str(yaml_data.get('taskDefinition').get('taskParams').get('ruleInputParameter').get('failure_strategy')),
        'operator': str(yaml_data.get('taskDefinition').get('taskParams').get('ruleInputParameter').get('operator')),
        'src_connector_type': int(yaml_data.get('taskDefinition').get('taskParams').get('ruleInputParameter').get('src_connector_type')),
        'src_datasource_id': int(yaml_data.get('taskDefinition').get('taskParams').get('ruleInputParameter').get('src_datasource_id')),
        'src_database': yaml_data.get('taskDefinition').get('taskParams').get('ruleInputParameter').get('src_database'),
        'src_table': yaml_data.get('taskDefinition').get('taskParams').get('ruleInputParameter').get('src_table'),
        'src_field': yaml_data.get('taskDefinition').get('taskParams').get('ruleInputParameter').get('src_field'),
        'src_filter': yaml_data.get('taskDefinition').get('taskParams').get('ruleInputParameter').get('src_filter'),
        'threshold': str(yaml_data.get('taskDefinition').get('taskParams').get('ruleInputParameter').get('threshold')),
    }
    if int(yaml_data.get('taskDefinition').get('taskParams').get('ruleInputParameter').get('comparison_type')) == 1:
        # if comparison type is fix value, set it.
        rule_input_parameter['comparison_name'] = str(yaml_data.get('taskDefinition').get('taskParams').get('ruleInputParameter').get('comparison_name'))
    spark_parameters = {
        'deployMode': yaml_data.get('taskDefinition').get('taskParams').get('sparkParameters').get('deployMode'),
        'driverCores': int(yaml_data.get('taskDefinition').get('taskParams').get('sparkParameters').get('driverCores')),
        'driverMemory': yaml_data.get('taskDefinition').get('taskParams').get('sparkParameters').get('driverMemory'),
        'executorCores': int(yaml_data.get('taskDefinition').get('taskParams').get('sparkParameters').get('executorCores')),
        'executorMemory': yaml_data.get('taskDefinition').get('taskParams').get('sparkParameters').get('executorMemory'),
        'numExecutors': int(yaml_data.get('taskDefinition').get('taskParams').get('sparkParameters').get('numExecutors')),
        'others': yaml_data.get('taskDefinition').get('taskParams').get('sparkParameters').get('others'),
        'yarnQueue': '',
    }
    task_params = {
        'resourceList': [],
        'localParams':[],
        'ruleId': 1,
        'ruleInputParameter': rule_input_parameter,
        'sparkParameters': spark_parameters,
    }
    task_definition = [
        {
            'code': task_code,
            'name': yaml_data.get('taskDefinition').get('name'),
            'description': yaml_data.get('taskDefinition').get('description'),
            'taskType': 'DATA_QUALITY',
            'taskParams': json.dumps(task_params),
            'flag': yaml_data.get('taskDefinition').get('flag'),
            'isCache': yaml_data.get('taskDefinition').get('isCache'),
            'taskPriority': yaml_data.get('taskDefinition').get('taskPriority'),
            'workerGroup': yaml_data.get('taskDefinition').get('workerGroup'),
            'environmentCode': -1,
            'failRetryTimes': int(yaml_data.get('taskDefinition').get('failRetryTimes')),
            'failRetryInterval': yaml_data.get('taskDefinition').get('failRetryInterval'),
            'timeoutFlag': yaml_data.get('taskDefinition').get('timeoutFlag'),
            'timeoutNotifyStrategy': yaml_data.get('taskDefinition').get('timeoutNotifyStrategy'),
            'timeout': int(yaml_data.get('taskDefinition').get('timeout')),
            'delayTime': int(yaml_data.get('taskDefinition').get('delayTime')),
            'cpuQuota': -1,
            'memoryMax': -1,
            }
        ]
    other_params = ''
    execution_type = "PARALLEL"

    params = {
        'name': name,
        'description': description,
        'globalParams': global_params,
        'locations': locations,
        'timeout': timeout,
        'taskRelationJson': json.dumps(task_relation),
        'taskDefinitionJson': json.dumps(task_definition),
        'otherParamsJson': json.dumps(other_params),
        'executionType': execution_type,
    }
    
    response = requests.post(url, headers=headers, params=params)
    status_code = response.status_code
    if status_code not in (HTTPStatus.CREATED, HTTPStatus.OK):
        print(f'Request failed, status: {status_code}')
        sys.exit(1)
    
    json_data = response.json()
    success = json_data.get('success')
    failed = json_data.get('failed')
    if (not success) or failed:
        code = json_data.get('code')
        msg = json_data.get('msg')
        print(f'Create failed, code: {code}, msg: {msg}')
        sys.exit(1)
    
    data = json_data.get('data')
    print(data)
    