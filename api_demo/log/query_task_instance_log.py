#!/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import requests


if __name__ == '__main__':
    server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
    user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')
    
    if len(sys.argv) < 2:
        print("Usage: {} <task-instance-id>".format(sys.argv[0]))
        sys.exit(1)
    
    task_instance_id = sys.argv[1]
    
    url = os.path.join(server_url, 'log', 'detail')
    headers = {'token': user_token}
    
    skip_line_num = 0
    limit = 2000
    log = ""
    while True:
        params = {
            "taskInstanceId": task_instance_id,
            "skipLineNum": skip_line_num,
            "limit": limit,
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
            print(f'Query task instance log failed, code: {code}, msg: {msg}')
            sys.exit(1)
        
        data = json_data.get('data')
        message = data.get('message')
        if message is None or len(message) == 0:
            break
        log += message
        
        skip_line_num = data.get('lineNum')
    
    print(log)
    