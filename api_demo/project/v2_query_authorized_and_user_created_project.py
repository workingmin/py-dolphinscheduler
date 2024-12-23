#!/bin/env python3
# -*- coding: utf-8 -*-

from http import HTTPStatus
import os
import requests


if __name__ == '__main__':    
    server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
    user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')

    url = os.path.join(server_url, 'v2', 'projects', 'created-and-authed')
    headers = {'token': user_token}
    
    response = requests.get(url, headers=headers)
    status_code = response.status_code
    if status_code == HTTPStatus.OK:
        json_data = response.json()
        success = json_data.get('success')
        if success:
            data = json_data.get('data')
            for project in data:
                print(project)
        
        failed = json_data.get('failed')
        if failed:
            code = json_data.get('code')
            msg = json_data.get('msg')
            print(f'Query failed, code: {code}, msg: {msg}')
    else:
        print(f'Request failed, status: {status_code}')