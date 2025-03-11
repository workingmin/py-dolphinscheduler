#!/bin/env python3
# -*- coding: utf-8 -*-


import datetime
from http import HTTPStatus
import os
import sys
import requests

PAGE_SIZE = 100

if __name__ == '__main__':
    server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
    user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')
    
    if len(sys.argv) < 2:
        print("Usage: {} <process-instance-id>".format(sys.argv[0]))
        sys.exit(1)
        
    process_instance_id = sys.argv[1]

    url = os.path.join(server_url, 'data-quality', 'result', 'page')
    headers = {'token': user_token}
    
    current_time = datetime.datetime.now()
    start_date = (current_time - datetime.timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    # XXX: Resilient to service time fluctuations
    end_date = (current_time + datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    page_no = 1

    while True:
        params = {
            "startDate": start_date,
            "endDate": end_date,
            "pageNo": page_no,
            "pageSize": PAGE_SIZE,
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
        total_list = data.get('totalList')
        is_continue = True
        for result in total_list:
            # XXX: Suppose a process instance does not have multiple data quality tasks
            if result.get('processInstanceId') == int(process_instance_id):
                print(result)
                is_continue = False
                break
        
        if not is_continue:
            break
        
        total_page = data.get('totalPage')
        current_page = data.get('currentPage')
        if total_page <= current_page:
            break
        
        page_no = current_page + 1