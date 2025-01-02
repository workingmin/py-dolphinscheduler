#!/bin/env python3
# -*- coding: utf-8 -*-

from http import HTTPStatus
import os
import sys
import requests


if __name__ == '__main__':
    server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
    user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')
    
    if len(sys.argv) < 3:
        print("Usage: {} <datasource-id> <database>".format(sys.argv[0]))
        sys.exit(1)
        
    datasource_id = sys.argv[1]
    database = sys.argv[2]
    
    url = os.path.join(server_url, 'datasources', 'tables')
    headers = {'token': user_token}
    params = {
        "datasourceId": datasource_id,
        "database": database,
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
        print(f'Get failed, code: {code}, msg: {msg}')
        sys.exit(1)
        
    data = json_data.get('data')
    for table in data:
        print(table)
    