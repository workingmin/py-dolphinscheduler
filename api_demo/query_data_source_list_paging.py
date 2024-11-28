#!/bin/env python3
# -*- coding: utf-8 -*-

from http import HTTPStatus
import os
import requests


if __name__ == '__main__':
    server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
    user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')
    
    url = os.path.join(server_url, 'datasources')
    headers = {'token': user_token}
    name = 'txx'
    params={
        "searchVal": name,
        "pageNo": 1,
        "pageSize": 100,
        }
    
    response = requests.get(url, headers=headers, params=params)
    status_code = response.status_code
    if status_code == HTTPStatus.OK:
        json_data = response.json()
        success = json_data.get('success')
        if success:
            data = json_data.get('data')
            data_sources = data.get('totalList')
            for data_source in data_sources:
                if data_source.get('name') == name:
                    id = data_source.get('id')
                    print(f"data source id: {id}")
                    break
        
        failed = json_data.get('failed')
        if failed:
            code = json_data.get('code')
            msg = json_data.get('msg')
            print(f'(code: {code}, msg: {msg})')
    else:
        print(f'Request failed, status: {status_code}')