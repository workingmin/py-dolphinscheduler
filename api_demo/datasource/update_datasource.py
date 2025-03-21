#!/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import requests


if __name__ == '__main__':
    server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
    user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')
    
    if len(sys.argv) < 2:
        print("Usage: {} <datasource-id>".format(sys.argv[0]))
        sys.exit(1)
        
    datasource_id = sys.argv[1]
    
    url = os.path.join(server_url, 'datasources', datasource_id)
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
    
    try:
        response = requests.put(url, headers=headers, json=data)
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
        print(f'Update failed, code: {code}, msg: {msg}')
        sys.exit(1)
    
    data = json_data.get('data')
    print(data)
