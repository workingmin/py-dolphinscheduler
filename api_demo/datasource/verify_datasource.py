#!/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import requests


if __name__ == '__main__':
    server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
    user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')
    
    if len(sys.argv) < 2:
        print("Usage: {} <datasource-name>".format(sys.argv[0]))
        sys.exit(1)
        
    datasource_name = sys.argv[1]

    url = os.path.join(server_url, 'datasources', 'verify-name')
    headers = {'token': user_token}
    params = {
        "name": datasource_name,
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
        print(f'Verify failed, code: {code}, msg: {msg}')
        sys.exit(1)
        
    data = json_data.get('data')
    print(f"Verifie {data}, data source name does not exist")
