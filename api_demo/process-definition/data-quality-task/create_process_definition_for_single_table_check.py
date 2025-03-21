#!/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum
import json
import os
import sys
import requests
import yaml

class Rule(Enum):
    NULL_CHECK = 1
    FIELD_LENGTH_CHECK = 5
    '''
        SELECT ${src_field} FROM ${src_database}.${src_table} group by ${src_field} having count(*) > 1 where ${src_filter}
        SELECT COUNT(*) AS duplicates FROM duplicate_items
        统计检查字段的重复值数，不是统计检查字段的重复总行数
    '''
    UNIQUENESS_CHECK = 6
    REGEXP_CHECK = 7
    '''
        SELECT * FROM ${src_database}.${src_table} where (${src_field} not in ( ${enum_list} ) or ${src_field} is null)
        SELECT COUNT(*) AS enums FROM enum_items
        检查某字段的非空值是否在枚举值的范围内
    '''
    ENUMERATION_CHECK = 9
    TABLE_COUNT_CHECK = 10

server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')

def gen_task_code(project_code):
    # XXX: Depend on other API that query task code list
    url = os.path.join(server_url, 'projects', project_code, 'task-definition', 'gen-task-codes')
    headers = {'token': user_token}
    params = {
        'genNum': 1
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        json_data = response.json()
    except Exception as e:
        print(f'Request failed, error: {e}. at {sys._getframe().f_lineno} in {sys._getframe().f_code.co_name}')
        return None

    success = json_data.get('success')
    failed = json_data.get('failed')
    if (not success) or failed:
        code = json_data.get('code')
        msg = json_data.get('msg')
        print(f'Query task code failed, code: {code}, msg: {msg}')
        return None
    
    task_code = json_data.get('data')[0]
    return task_code

def get_rule_form_create_json_keys(rule_id):
    # XXX: Depend on other API that get rule form-create json
    url = os.path.join(server_url, 'data-quality', 'getRuleFormCreateJson')
    headers = {'token': user_token}
    params = {
        "ruleId": rule_id,
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        json_data = response.json()
    except Exception as e:
        print(f'Request failed, error: {e}. at {sys._getframe().f_lineno} in {sys._getframe().f_code.co_name}')
        return None

    success = json_data.get('success')
    failed = json_data.get('failed')
    if (not success) or failed:
        code = json_data.get('code')
        msg = json_data.get('msg')
        print(f'Query failed, code: {code}, msg: {msg}')
        return None
    
    data = json_data.get('data')
    form_create_json = json.loads(data)
    
    return [ x.get('field') for x in form_create_json ]


if __name__ == '__main__':    
    if len(sys.argv) < 3:
        print("Usage: {} <project-code> <process-definition-params.yaml>".format(sys.argv[0]))
        sys.exit(1)
    
    project_code =  sys.argv[1]
    with open(sys.argv[2], 'r') as f:
        yaml_data = yaml.safe_load(f)
    
    rule_id = int(yaml_data.get('taskDefinition').get('taskParams').get('ruleId'))
    if rule_id not in [member.value for member in Rule.__members__.values()]:
        print(f"Invalid data quality rule id: {rule_id}")
        sys.exit(1)
        
    task_code = gen_task_code(project_code)
    if task_code is None or task_code == 0:
        sys.exit(1)
        
    rule_input_parameter_keys = get_rule_form_create_json_keys(rule_id)
    if rule_input_parameter_keys is None or len(rule_input_parameter_keys) == 0:
        sys.exit(1)
        
    # Create process definition
    url = os.path.join(server_url, 'projects', project_code, 'process-definition')
    headers = {'token': user_token}
        
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
    
    # According to /data-quality/getRuleFormCreateJson?ruleId=${rule_id}
    rule_input_parameter = dict()
    for k in rule_input_parameter_keys:
        if k in ['src_connector_type', 'src_datasource_id', 'comparison_type']:
            rule_input_parameter[k] = int(yaml_data.get('taskDefinition').get('taskParams').get('ruleInputParameter').get(k))
        elif k in ['check_type', 'operator', 'threshold', 'failure_strategy', 'field_length']:
            rule_input_parameter[k] = str(yaml_data.get('taskDefinition').get('taskParams').get('ruleInputParameter').get(k))
        else:
            rule_input_parameter[k] = yaml_data.get('taskDefinition').get('taskParams').get('ruleInputParameter').get(k)
            
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
        'ruleId': rule_id,
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
    
    try:
        response = requests.post(url, headers=headers, params=params)
        response.raise_for_status()
        json_data = response.json()
    except Exception as e:
        print(f'Request failed, error: {e}. at {sys._getframe().f_lineno} in {sys._getframe().f_code.co_name}')
        sys.exit(1)

    success = json_data.get('success')
    failed = json_data.get('failed')
    if (not success) or failed:
        code = json_data.get('code')
        msg = json_data.get('msg')
        print(f'Create failed, code: {code}, msg: {msg}')
        sys.exit(1)
    
    data = json_data.get('data')
    print(data)
    