#!/bin/env python3
# -*- coding: utf-8 -*-

from http import HTTPStatus
import os
import requests


if __name__ == '__main__':
    server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
    user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')
    
    url = os.path.join(server_url, 'datasources')
    headers = {
        'Content-type': 'application/json',
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
        "other":{
            "serverTimezone":"GMT-8"
            },
        }
        
    response = requests.request('post', url, headers=headers, json=data)
    status_code = response.status_code
    if status_code in (HTTPStatus.CREATED, HTTPStatus.OK):
        json_data = response.json()
        print(json_data)
    else:
        print(f'Request failed, status: {status_code}')
