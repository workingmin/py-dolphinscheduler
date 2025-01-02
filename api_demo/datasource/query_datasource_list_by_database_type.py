#!/bin/env python3
# -*- coding: utf-8 -*-

from http import HTTPStatus
import os
import sys
import requests


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: {} <database-type>".format(sys.argv[0]))
        print("Available values : MYSQL, POSTGRESQL, HIVE, SPARK, " \
            "CLICKHOUSE, ORACLE, SQLSERVER, DB2, PRESTO, H2, REDSHIFT, " \
            "ATHENA, TRINO, STARROCKS, AZURESQL, DAMENG, OCEANBASE, SSH, " \
            "KYUUBI, DATABEND, SNOWFLAKE, VERTICA, HANA, DORIS")
        sys.exit(1)
        
    database_type = sys.argv[1]
    
    server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
    user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')
    
    url = os.path.join(server_url, 'datasources', 'list')
    headers = {'token': user_token}
    params = {
        "type": database_type.upper(),
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
        print(f'Query failed, code: {code}, msg: {msg}')
        sys.exit(1)
        
    data = json_data.get('data')
    for datasource in data:
        print(datasource)
