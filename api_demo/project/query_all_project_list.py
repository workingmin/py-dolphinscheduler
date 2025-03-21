#!/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import requests


if __name__ == '__main__':    
    server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
    user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')
    
    url = os.path.join(server_url, 'projects', 'list')
    headers = {'token': user_token}

    try:
        response = requests.get(url, headers=headers)
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
        print(f'Query failed, code: {code}, msg: {msg}')
     
    data = json_data.get('data')
    for project in data:
        print(project)
        