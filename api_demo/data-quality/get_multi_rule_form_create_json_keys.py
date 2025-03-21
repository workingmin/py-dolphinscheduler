#!/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import sys
import requests


if __name__ == '__main__':
    server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
    user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')
    
    if len(sys.argv) < 2:
        print("Usage: {} <id1,id2,...>".format(sys.argv[0]))
        sys.exit(1)
        
    url = os.path.join(server_url, 'data-quality', 'getRuleFormCreateJson')
    headers = {'token': user_token}
    
    rule_ids = (sys.argv[1]).split(',')
    keys = []
    for rule_id in rule_ids:
        params = {
            "ruleId": rule_id,
        }
    
        try:
            response = requests.get(url, headers=headers, params=params)
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
            sys.exit(1)
    
        data = json_data.get('data')
        form_create_json = json.loads(data)
    
        keys.extend([ x.get('field') for x in form_create_json ])
    
    print(list(dict.fromkeys(keys)))
