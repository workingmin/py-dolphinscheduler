#!/bin/env python3
# -*- coding: utf-8 -*-

from http import HTTPStatus
import os
import requests


if __name__ == '__main__':
    server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
    user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')
    
    url = os.path.join(server_url, 'v2/projects')
    headers = {
        'Content-type': 'application/json',
        'token': user_token
        }
    
    data = {
        "projectName": "pro123",
        "description": "this is a project"
        }

    response = requests.post(url, headers=headers, json=data)
    status_code = response.status_code
    if status_code in (HTTPStatus.CREATED, HTTPStatus.OK):
        json_data = response.json()
        print(json_data)
    else:
        print(f'Request failed, status: {status_code}')