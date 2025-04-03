#!/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import requests
import yaml


if __name__ == '__main__':
    server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
    user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')
    
    if len(sys.argv) < 2:
        print("Usage: {} <datasource-param.yaml>".format(sys.argv[0]))
        sys.exit(1)
        
    with open(sys.argv[1], 'r') as f:
        datasource_param = yaml.safe_load(f)
        
    url = os.path.join(server_url, 'datasources', 'connect')
    headers = {
        'Content-Type': 'application/json',
        'token': user_token
        }
    
    data = datasource_param
    
    try:
        response = requests.post(url, headers=headers, json=data)
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
        print(f'Connect failed, code: {code}, msg: {msg}')
        sys.exit(1)
    
    data = json_data.get('data')
    if data:
        print('Connect datasource success')
    else:
        print('Connect datasource failure')