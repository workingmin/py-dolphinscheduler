#!/bin/env python3
# -*- coding: utf-8 -*-

from http import HTTPStatus
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
    
    url = os.path.join(server_url, 'projects', project_code, 'process-definition', process_definition_code)
    headers = {'token': user_token}
    
    response = requests.delete(url, headers=headers)
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
        print(f'Delete failed, code: {code}, msg: {msg}')
        sys.exit(1)
    
    print("Deleted success")
    