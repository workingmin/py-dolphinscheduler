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
        print("Usage: {} <project-code>".format(sys.argv[0]))
        sys.exit(1)
    
    project_code = sys.argv[1]

    url = os.path.join(server_url, 'projects', project_code)
    headers = {'token': user_token}
    params = {
        "projectName": "pro123",
        "description": "this is a project"
        }
    
    response = requests.put(url, headers=headers, params=params)
    status_code = response.status_code
    if status_code == HTTPStatus.OK:
        json_data = response.json()
        success = json_data.get('success')
        if success:
            data = json_data.get('data')
            print(data)
        
        failed = json_data.get('failed')
        if failed:
            code = json_data.get('code')
            msg = json_data.get('msg')
            print(f'Update failed, code: {code}, msg: {msg}')
    else:
        print(f'Request failed, status: {status_code}')
    