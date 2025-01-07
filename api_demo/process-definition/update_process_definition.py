#!/bin/env python3
# -*- coding: utf-8 -*-

from http import HTTPStatus
import json
import os
import sys
import requests


if __name__ == '__main__':
    server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
    user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')
    
    if len(sys.argv) < 2:
        print("Usage: {} <project-code> <process-definition-code>".format(sys.argv[0]))
        sys.exit(1)
    
    project_code = sys.argv[1]
    process_definition_code = sys.argv[2]
    
    # XXX: Depend on another API that query task code list
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
    
    data = json_data.get('data')
    task_code = data[0]
    
    # Update process definition
    url = os.path.join(server_url, 'projects', project_code, 'process-definition', process_definition_code)
    
    name = "dag_test"
    description = "desc test"
    global_params = "[]"
    locations = "[]"
    timeout = 0
    task_relation = [
        {
            'name': '',
            'preTaskCode': 0,
            'preTaskVersion': 0,
            'postTaskCode': task_code,
            'postTaskVersion': 2,
            'conditionType': 'NONE',
            'conditionParams': {},
            },
        ]
    task_params = {
        'resourceList': [],
        'localParams':[
            {
                'prop': 'datetime',
                'direct': 'IN',
                'type': 'VARCHAR',
                'value': '${system.datetime}'
                }
            ],
        'rawScript': 'echo ${datetime}',
        'conditionResult': {
            'successNode': [''],
            'failedNode': ['']
            },
        'dependence': {}
    }
    task_definition = [
        {
            'code': task_code,
            'name': 'detail_up',
            'description': '',
            'taskType': 'SHELL',
            'taskParams': json.dumps(task_params),
            'flag': 'YES',
            'isCache': 'NO',
            'taskPriority': 0,
            'workerGroup': 'default',
            'failRetryTimes': 0,
            'failRetryInterval': 0,
            'timeoutFlag': 0,
            'timeoutNotifyStrategy': 0,
            'timeout': 0,
            'delayTime': 0,
            'resourceIds': ''
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
    
    response = requests.put(url, headers=headers, params=params)
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
        print(f'Update failed, code: {code}, msg: {msg}')
        sys.exit(1)
    
    data = json_data.get('data')
    print(data)
    