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
        print("Usage: {} <datasource-name>".format(sys.argv[0]))
        sys.exit(1)
        
    datasource_name = sys.argv[1]

    url = os.path.join(server_url, 'datasources', 'verify-name')
    headers = {'token': user_token}
    params = {
        "name": datasource_name,
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
        print(f'Verify failed, code: {code}, msg: {msg}')
        sys.exit(1)
        
    data = json_data.get('data')
    print(f"Verifie {data}, data source name does not exist")
