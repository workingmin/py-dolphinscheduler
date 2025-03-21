#!/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import requests


if __name__ == '__main__':
    server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
    user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')
    
    if len(sys.argv) < 3:
        print("Usage: {} <project-code> <process-definition-code>".format(sys.argv[0]))
        sys.exit(1)
    
    project_code = sys.argv[1]
    process_definition_code = sys.argv[2]

    url = os.path.join(server_url, 'projects', project_code, 'process-instances')
    headers = {'token': user_token}
    params = {
        "processDefineCode": process_definition_code,
        "pageNo": 1,
        "pageSize": 10,
    }
        
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        json_data = response.json()
    except Exception as e:
        print(f'Request failed, error: {e}')
        sys.exit(1)
    
    success = json_data.get('success')
    failed = json_data.get('failed')
    if (not success) or failed:
        code = json_data.get('code')
        msg = json_data.get('msg')
        print(f'Query process instance failed, code: {code}, msg: {msg}')
        sys.exit(1)
        
    data = json_data.get('data')
    total = data.get('total')
    total_page = data.get('totalPage')
    page_size = data.get('pageSize')
    current_page = data.get('currentPage')
    page_no = data.get('pageNo')
    print(f'total: {total}, total page: {total_page}, page size: {page_size}, current page: {current_page}, page no: {page_no}.\n')
    
    total_list = data.get('totalList')
    for process_instance in total_list:
        process_instance = {k: v for k, v in process_instance.items() \
            if k in set(['id', 'processDefinitionCode', 'projectCode', 'state', 'recovery',
                         'startTime', 'endTime', 'runTimes', 'name', 'commandType', 'scheduleTime', 'duration', 'dryRun'])}
        
        # XXX: query task list by process instance id
        process_instance_id = process_instance.get('id')
        url = os.path.join(server_url, 'projects', project_code, 'process-instances', str(process_instance_id), 'tasks')
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            json_data = response.json()
        except Exception as e:
            print(f'Request failed, error: {e}')
            sys.exit(1)

        success = json_data.get('success')
        failed = json_data.get('failed')
        if (not success) or failed:
            code = json_data.get('code')
            msg = json_data.get('msg')
            print(f'Query tasks failed, code: {code}, msg: {msg}')
            sys.exit(1)
            
        task_list = json_data.get('data').get('taskList')
        task_list = [ {k: v for k, v in task.items() if k in set(['id', 'name', 'taskType', 'taskCode'])} \
            for task in task_list ]
        
        process_instance['taskList'] = task_list
        print(process_instance)
        