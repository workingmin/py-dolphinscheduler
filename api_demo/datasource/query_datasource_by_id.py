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
        print("Usage: {} <datasource-id>".format(sys.argv[0]))
        sys.exit(1)
    
    datasource_id = sys.argv[1]
    
    url = os.path.join(server_url, 'datasources', datasource_id)
    headers = {'token': user_token}
    
    response = requests.get(url, headers=headers)
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
            print(f'query failed, code: {code}, msg: {msg}')
    else:
        print(f'Request failed, status: {status_code}')