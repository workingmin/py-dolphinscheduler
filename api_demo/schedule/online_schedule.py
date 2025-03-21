#!/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import requests


if __name__ == '__main__':
    server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
    user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')
    
    if len(sys.argv) < 3:
        print("Usage: {} <project-code> <schedule-id>".format(sys.argv[0]))
        sys.exit(1)
        
    project_code = sys.argv[1]
    schedule_id = sys.argv[2]

    url = os.path.join(server_url, 'projects', project_code, 'schedules', schedule_id, 'online')
    headers = {'token': user_token}
    
    try:
        response = requests.post(url, headers=headers)
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
        print(f'Online failed, code: {code}, msg: {msg}')
        sys.exit(1)

    data = json_data.get('data')
    print(data)
    