#!/bin/env python3
# -*- coding: utf-8 -*-

from http import HTTPStatus
import os
import sys
import requests


if __name__ == '__main__':
    server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
    user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')
    
    url = os.path.join(server_url, 'datasources')
    headers = {
        'Content-Type': 'application/json',
        'token': user_token
        }
    data = {
        "type": "MYSQL",
        "name": "txx",
        "note": "",
        "host": "localhost",
        "port": 3306,
        "userName": "root",
        "password": "xxx",
        "database": "ds",
        "other": {
            "serverTimezone":"GMT-8"
            },
        }
        
    response = requests.post(url, headers=headers, json=data)
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
