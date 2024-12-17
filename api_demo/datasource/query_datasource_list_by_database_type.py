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
    
    url = os.path.join(server_url, 'datasources/list')
    headers = {'token': user_token}
    response = requests.get(url, headers=headers, params={"type": database_type})
    status_code = response.status_code
    if status_code == HTTPStatus.OK:
        json_data = response.json()
        success = json_data.get('success')
        if success:
            data = json_data.get('data')
            for datasource in data:
                print(datasource)
        
        failed = json_data.get('failed')
        if failed:
            code = json_data.get('code')
            msg = json_data.get('msg')
            print(f'query failed, code: {code}, msg: {msg}')
    else:
        print(f'Request failed, status: {status_code}')
    