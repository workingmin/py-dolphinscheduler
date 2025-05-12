#!/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum, IntEnum
import hashlib
import json
import os
import sys
import time

import requests
import yaml


server_url = os.getenv('DOLPHINSCHEDULER_SERVER_URL')
user_token = os.getenv('DOLPHINSCHEDULER_USER_TOKEN')

class ConnectorType(IntEnum):
    TRINO = 12

class RuleType(Enum):
    SINGLE_TABLE = (0, "single_table")
    
    def __init__(self, code, description):
        self.code = code
        self.description = description
    
    @classmethod
    def is_valid_rule_type(cls, rule_type):
        return any(rule_type == item.code for item in cls)
    
class Rule(Enum):
    NULL_CHECK = (1, '$t(null_check)', RuleType.SINGLE_TABLE.code, 'null_items', 'null_count', 'nulls')
    FIELD_LENGTH_CHECK = (5, '$t(field_length_check)', RuleType.SINGLE_TABLE.code, 'invalid_length_items', 'invalid_length_count', 'valids')
    UNIQUENESS_CHECK = (6, '$t(uniqueness_check)', RuleType.SINGLE_TABLE.code, 'duplicate_items', 'duplicate_count', 'duplicates')
    REGEXP_CHECK = (7, '$t(regexp_check)', RuleType.SINGLE_TABLE.code, 'regexp_items', 'invalid_format_count', 'invalids')
    ENUMERATION_CHECK = (9, '$t(enumeration_check)', RuleType.SINGLE_TABLE.code, 'enum_items', 'invalid_enumeration_count', 'invalids')
    TABLE_COUNT_CHECK = (10, '$t(table_count_check)', RuleType.SINGLE_TABLE.code, None, 'table_count', 'total')

    def __init__(self, id, display_name, rule_type, output_items_table, output_count_table, field_alias):
        self.id = id
        self.display_name = display_name
        self.rule_type = rule_type
        self.output_items_table = output_items_table
        self.output_count_table = output_count_table
        self.field_alias = field_alias
        self.statistics_name = f"{output_count_table}.{field_alias}"
    
    @classmethod
    def is_valid_rule_id(cls, rule_id):
        return any(rule_id == item.id for item in cls)
    
class ComparisonType(Enum):
    FixValue = (1, 'FixValue', None, None)
    DailyAvg = (2, 'DailyAvg', 'day_range', 'day_avg')
    WeeklyAvg = (3, 'WeeklyAvg', 'week_range', 'week_avg')
    MonthlyAvg = (4, 'MonthlyAvg', 'month_range', 'month_avg')
    Last7DayAvg = (5, 'Last7DayAvg', 'last_seven_days', 'last_7_avg')
    Last30DayAvg = (6, 'Last30DayAvg', 'last_thirty_days', 'last_30_avg')
    
    def __init__(self, id, display_name, output_table, output_column):
        self.id = id
        self.display_name = display_name
        self.output_table = output_table
        self.output_column = output_column
        
    @classmethod
    def is_valid_comparison_type(cls, comparison_type_id):
        return any(comparison_type_id == getattr(cls, attr).id for attr in dir(cls) if isinstance(getattr(cls, attr), cls))

def generate_unique_code(src_table, src_connector_type, src_datasource_id, src_field, src_database, rule_id, statistics_name, src_filter):
    s = f'{src_table}{src_connector_type}{src_datasource_id}{src_field}{src_database}{rule_id}{statistics_name}{src_filter}'
    md5_hash = hashlib.md5()
    md5_hash.update(s.encode('utf-8'))
    unique_code = md5_hash.hexdigest()
    
    current_time = int(time.time() * 1000)  # 毫秒时间戳
    combined = f"{unique_code}{current_time}".encode('utf-8')
    task_hash = hashlib.sha256(combined).hexdigest()
    task_unique_code = "".join(c for c in task_hash if c.isalpha())[:6].lower()
    
    return unique_code, task_unique_code

def build_trnio_sql(rule_id, rule_input_parameter):
    if not isinstance(rule_id, int) or not isinstance(rule_input_parameter, dict):
        print("Invalid parameter types")
        return None

    src_connector_type = rule_input_parameter.get('src_connector_type')
    if src_connector_type != ConnectorType.TRINO.value:
        print(f"Not trino connector type: {src_connector_type}")
        return None

    if not Rule.is_valid_rule_id(rule_id):
        print(f"Invalid data quality rule id: {rule_id}")
        return None

    env_vars = {
        'dest_catalog': os.getenv('TRNIO_DQ_DATACATALOG'),
        'dest_database': os.getenv('TRNIO_DQ_DATABASE'),
        'dest_dq_execute_result_table': os.getenv('TRNIO_DQ_EXECUTE_RESULT_TABLE'),
        'dest_dq_task_statistics_value_table': os.getenv('TRNIO_DQ_TASK_STATISTICS_VALUE_TABLE')
    }
    if any(v is None for v in env_vars.values()):
        print("Missing required environment variables")
        return None

    rule = next((item for item in Rule if item.id == rule_id), None)
    if not rule:
        print(f"Invalid rule id: {rule_id}")
        return None
 
    rule_required_params = {
        Rule.NULL_CHECK.id: ['src_connector_type', 'src_datasource_id', 'src_catalog', 'src_database', 'src_table', 'src_field', 'check_type', 'operator', 'threshold', 'failure_strategy', 'comparison_type'],
        Rule.FIELD_LENGTH_CHECK.id: ['src_connector_type', 'src_datasource_id', 'src_catalog', 'src_database', 'src_table', 'src_field', 'logic_operator', 'field_length', 'check_type', 'operator', 'threshold', 'failure_strategy', 'comparison_type'],
        Rule.UNIQUENESS_CHECK.id: ['src_connector_type', 'src_datasource_id', 'src_catalog', 'src_database', 'src_table', 'src_field', 'check_type', 'operator', 'threshold', 'failure_strategy', 'comparison_type'],
        Rule.REGEXP_CHECK.id: ['src_connector_type', 'src_datasource_id', 'src_catalog', 'src_database', 'src_table', 'src_field', 'regexp_pattern', 'check_type', 'operator', 'threshold', 'failure_strategy', 'comparison_type'],
        Rule.ENUMERATION_CHECK.id: ['src_connector_type', 'src_datasource_id', 'src_catalog', 'src_database', 'src_table', 'src_field', 'enum_list', 'check_type', 'operator', 'threshold', 'failure_strategy', 'comparison_type'],
        Rule.TABLE_COUNT_CHECK.id: ['src_connector_type', 'src_datasource_id', 'src_catalog', 'src_database', 'src_table', 'check_type', 'operator', 'threshold', 'failure_strategy', 'comparison_type']
    }
    required_params = rule_required_params[rule_id]
    if not all(p in rule_input_parameter for p in required_params):
        print("Missing required parameters")
        return None
 
    unique_code, task_unique_code = generate_unique_code(
        rule_input_parameter['src_table'],
        src_connector_type,
        rule_input_parameter['src_datasource_id'],
        rule_input_parameter['src_field'],
        rule_input_parameter['src_database'],
        rule_id,
        rule.statistics_name,
        rule_input_parameter.get('src_filter')
    )

    def build_base_count_sql(condition, output_items_table, output_count_table, field_alias):
        src_filter = rule_input_parameter.get('src_filter')
        filter_clause = f" AND ({src_filter})" if src_filter else ""
        
        return (
            f"CREATE TABLE IF NOT EXISTS {env_vars['dest_catalog']}.{env_vars['dest_database']}.{output_items_table} "
            f"AS SELECT * FROM {rule_input_parameter['src_catalog']}.{rule_input_parameter['src_database']}."
            f"{rule_input_parameter['src_table']} WHERE {condition}{filter_clause};\n"
            f"CREATE TABLE IF NOT EXISTS {env_vars['dest_catalog']}.{env_vars['dest_database']}.{output_count_table} "
            f"AS SELECT COUNT(*) AS {field_alias} "
            f"FROM {env_vars['dest_catalog']}.{env_vars['dest_database']}.{output_items_table};\n"
        )

    output_items_table = f"{rule.output_items_table}_{task_unique_code}"
    output_count_table = f"{rule.output_count_table}_{task_unique_code}"
    
    rule_handlers = {
        Rule.NULL_CHECK.id: lambda: build_base_count_sql(
            f"({rule_input_parameter['src_field']} is null or {rule_input_parameter['src_field']} = '')",
            output_items_table,
            output_count_table,
            rule.field_alias
        ),
        Rule.FIELD_LENGTH_CHECK.id: lambda: build_base_count_sql(
            f"LENGTH({rule_input_parameter['src_field']}) {rule_input_parameter.get('logic_operator')} {rule_input_parameter.get('field_length')}",
            output_items_table,
            output_count_table,
            rule.field_alias
        ),
        Rule.UNIQUENESS_CHECK.id: lambda: (
            f"CREATE TABLE IF NOT EXISTS {env_vars['dest_catalog']}.{env_vars['dest_database']}.{output_items_table} "
            f"AS SELECT {rule_input_parameter['src_field']} FROM {rule_input_parameter['src_catalog']}.{rule_input_parameter['src_database']}."
            f"{rule_input_parameter['src_table']} GROUP BY {rule_input_parameter['src_field']} HAVING COUNT(*) > 1"
            f"{' AND (' + rule_input_parameter.get('src_filter') + ')' if rule_input_parameter.get('src_filter') else ''};\n"
            f"CREATE TABLE IF NOT EXISTS {env_vars['dest_catalog']}.{env_vars['dest_database']}.{output_count_table} "
            f"AS SELECT COUNT(*) AS {rule.field_alias} "
            f"FROM {env_vars['dest_catalog']}.{env_vars['dest_database']}.{output_items_table};\n"
        ),
        Rule.REGEXP_CHECK.id: lambda: build_base_count_sql(
            f"({rule_input_parameter['src_field']} not regexp '{rule_input_parameter.get('regexp_pattern')}')",
            output_items_table,
            output_count_table,
            rule.field_alias
        ),
        Rule.ENUMERATION_CHECK.id: lambda: build_base_count_sql(
            f"({rule_input_parameter['src_field']} NOT IN ({rule_input_parameter.get('enum_list')}) OR {rule_input_parameter['src_field']} IS NULL)",
            output_items_table,
            output_count_table,
            rule.field_alias
        ),
        Rule.TABLE_COUNT_CHECK.id: lambda: (
            f"CREATE TABLE IF NOT EXISTS {env_vars['dest_catalog']}.{env_vars['dest_database']}.{output_count_table} "
            f"AS SELECT COUNT(*) AS {rule.field_alias} "
            f"FROM {rule_input_parameter['src_catalog']}.{rule_input_parameter['src_database']}.{rule_input_parameter['src_table']}"
            f"{' WHERE (' + rule_input_parameter.get('src_filter') + ')' if rule_input_parameter.get('src_filter') else ''};\n"
        )
    }

    sql = rule_handlers[rule_id]()

    def build_comparison_sql(date_clause, output_comparison_table, output_comparison_column, unique_code, statistics_name):
        return (
            f"CREATE TABLE IF NOT EXISTS {env_vars['dest_catalog']}.{env_vars['dest_database']}.{output_comparison_table} AS "
            f"SELECT IFNULL(ROUND(AVG(statistics_value), 2)) AS {output_comparison_column} "
            f"FROM {env_vars['dest_catalog']}.{env_vars['dest_database']}.{env_vars['dest_dq_task_statistics_value_table']} "
            f"WHERE {date_clause} AND unique_code = '{unique_code}' AND statistics_name = '{statistics_name}';\n"
        )

    date_ranges = {
        ComparisonType.DailyAvg.id: ("day", "DATE_ADD('day', -1, DATE_TRUNC('day', ${{data_time}}))", "DATE_TRUNC('day', ${{data_time}})"),
        ComparisonType.WeeklyAvg.id: ("week", "DATE_ADD('week', -1, DATE_TRUNC('week', ${{data_time}}))", "DATE_TRUNC('week', ${{data_time}})"),
        ComparisonType.MonthlyAvg.id: ("month", "DATE_ADD('month', -1, DATE_TRUNC('month', ${{data_time}}))", "DATE_TRUNC('month', ${{data_time}})"),
        ComparisonType.Last7DayAvg.id: ("day", "DATE_ADD('day', -7, DATE_TRUNC('day', ${{data_time}}))", "DATE_TRUNC('day', ${{data_time}})"),
        ComparisonType.Last30DayAvg.id: ("day", "DATE_ADD('day', -30, DATE_TRUNC('day', ${{data_time}}))", "DATE_TRUNC('day', ${{data_time}})")
    }

    comparison_type_id = rule_input_parameter.get('comparison_type')
    output_comparison_table = None
    comparison_value = '0'

    if comparison_type_id and comparison_type_id in date_ranges:
        _, start_date, end_date = date_ranges[comparison_type_id]
        date_clause = f"data_time >= {start_date} AND data_time < {end_date}"

        comparison_type = next((item for item in ComparisonType if item.id == comparison_type_id), None)
        if comparison_type and comparison_type.output_table:
            output_comparison_table = f"{comparison_type.output_table}_{task_unique_code}"
            comparison_value = f"{env_vars['dest_catalog']}.{env_vars['dest_database']}.{output_comparison_table}.{comparison_type.output_column}"
            
            sql += build_comparison_sql(
                date_clause,
                output_comparison_table,
                comparison_type.output_column,
                unique_code,
                rule.statistics_name
            )
    elif comparison_type_id == ComparisonType.FixValue.id:
        comparison_value = rule_input_parameter.get('comparison_name', '0')

    sql += (
        f"INSERT INTO "
        f"{env_vars['dest_catalog']}.{env_vars['dest_database']}.{env_vars['dest_dq_execute_result_table']} ("
        f"rule_type, "
        f"rule_name, "
        f"process_definition_id, "
        f"process_instance_id, "
        f"task_instance_id, "
        f"statistics_value, "
        f"comparison_value, "
        f"comparison_type, "
        f"check_type, "
        f"threshold, "
        f"operator, "
        f"failure_strategy, "
        f"create_time, "
        f"update_time ) "
        f"SELECT "
        f"{rule.rule_type} AS rule_type, "
        f"'{rule.display_name}' AS rule_name, "
        f"${{system.workflow.definition.code}} AS process_definition_id, "
        f"${{system.workflow.instance.id}} AS process_instance_id, "
        f"${{system.task.instance.id}} AS task_instance_id, "
        f"t_count.{rule.field_alias} AS statistics_value, "
        f"{comparison_value} AS comparison_value, "
        f"{comparison_type_id} AS comparison_type, "
        f"{rule_input_parameter['check_type']} AS check_type, "
        f"{rule_input_parameter['threshold']} AS threshold, "
        f"{rule_input_parameter['operator']} AS operator, "
        f"{rule_input_parameter['failure_strategy']} AS failure_strategy, "
        f"NOW() AS create_time, "
        f"NOW() AS update_time "
        f"FROM {env_vars['dest_catalog']}.{env_vars['dest_database']}.{output_count_table} AS t_count"
    )

    if output_comparison_table:
        sql += f"FULL JOIN {env_vars['dest_catalog']}.{env_vars['dest_database']}.{output_comparison_table};\n"
    else:
        sql += ";\n"

    sql += (
        f"INSERT INTO "
        f"{env_vars['dest_catalog']}.{env_vars['dest_database']}.{env_vars['dest_dq_task_statistics_value_table']} ("
        f"process_definition_id, "
        f"task_instance_id, "
        f"rule_id, "
        f"unique_code, "
        f"statistics_name, "
        f"statistics_value, "
        f"data_time, "
        f"create_time, "
        f"update_time ) "
        f"SELECT "
        f"${{system.workflow.definition.code}} AS process_definition_id, "
        f"${{system.task.instance.id}} AS task_instance_id, "
        f"{rule_id} AS rule_id, "
        f"'{unique_code}' AS unique_code, "
        f"'{rule.statistics_name}' AS statistics_name, "
        f"t_count.{rule.field_alias} AS statistics_value, "
        f"NOW() AS data_time, "
        f"NOW() AS create_time, "
        f"NOW() AS update_time "
        f"FROM {env_vars['dest_catalog']}.{env_vars['dest_database']}.{output_count_table} AS t_count;\n"
    )

    sql += (
        f"DROP TABLE IF EXISTS {env_vars['dest_catalog']}.{env_vars['dest_database']}.{output_items_table};\n"
        f"DROP TABLE IF EXISTS {env_vars['dest_catalog']}.{env_vars['dest_database']}.{output_count_table};\n"
    )
    if output_comparison_table:
        sql += (
            f"DELETE FROM {env_vars['dest_catalog']}.{env_vars['dest_database']}.{output_comparison_table};\n"
        )

    return sql

def gen_task_code(project_code):
    # XXX: Depend on other API that query task code list
    url = os.path.join(server_url, 'projects', project_code, 'task-definition', 'gen-task-codes')
    headers = {'token': user_token}
    params = {
        'genNum': 1
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        json_data = response.json()
    except Exception as e:
        print(f'Request failed, error: {e}. at {sys._getframe().f_lineno} in {sys._getframe().f_code.co_name}')
        return None

    success = json_data.get('success')
    failed = json_data.get('failed')
    if (not success) or failed:
        code = json_data.get('code')
        msg = json_data.get('msg')
        print(f'Query task code failed, code: {code}, msg: {msg}')
        return None

    task_code = json_data.get('data')[0]
    return task_code


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: {} <project-code> <process-definition-params.yaml>".format(sys.argv[0]))
        sys.exit(1)
    
    project_code = sys.argv[1]
    process_definition_params_filepath = sys.argv[2]
    
    with open(process_definition_params_filepath, 'r') as file:
        process_definition_params = yaml.safe_load(file)
    
    rule_id = int(process_definition_params.get('taskDefinition').get('taskParams').get('ruleId'))
    rule_input_parameter = process_definition_params.get('taskDefinition').get('taskParams').get('ruleInputParameter')
    sql = build_trnio_sql(rule_id, rule_input_parameter)
    if sql is None or len(sql) == 0:
        print("Failed to build SQL.")
        sys.exit(1)
    
    task_code = gen_task_code(project_code)
    if task_code is None or task_code == 0:
        print(f"Failed to generate task code.")
        sys.exit(1)
        
    # Create process definition
    url = os.path.join(server_url, 'projects', project_code, 'process-definition')
    headers = {'token': user_token}
    
    name = process_definition_params.get('name')
    description = process_definition_params.get('description')
    global_params = "[]"
    locations = "[]"
    timeout = 0
    task_relation = [
        {
            'name': '',
            'preTaskCode': 0,
            'preTaskVersion': 0,
            'postTaskCode': task_code,
            'postTaskVersion': 1,
            'conditionType': 'NONE',
            'conditionParams': {},
            },
        ]
    
    src_datasource_id = rule_input_parameter.get('src_datasource_id')
    
    task_params = {
        'localParams': [],
        'resourceList': [],
        'type': 'TRINO',
        'datasource': src_datasource_id,
        'sql': sql,
        'sqlType': "1",
        'preStatements': [],
        'postStatements': [],
        'displayRows': 10
    }
    task_definition = [
        {
            'code': task_code,
            'name':  process_definition_params.get('taskDefinition').get('name'),
            'description': process_definition_params.get('taskDefinition').get('description'),
            'taskType': 'SQL',
            'taskParams': json.dumps(task_params),
            'flag': process_definition_params.get('taskDefinition').get('flag'),
            'isCache': process_definition_params.get('taskDefinition').get('isCache'),
            'taskPriority': process_definition_params.get('taskDefinition').get('taskPriority'),
            'workerGroup': process_definition_params.get('taskDefinition').get('workerGroup'),
            'environmentCode': -1,
            'failRetryTimes': int(process_definition_params.get('taskDefinition').get('failRetryTimes')),
            'failRetryInterval': process_definition_params.get('taskDefinition').get('failRetryInterval'),
            'timeoutFlag': process_definition_params.get('taskDefinition').get('timeoutFlag'),
            'timeoutNotifyStrategy': process_definition_params.get('taskDefinition').get('timeoutNotifyStrategy'),
            'timeout': int(process_definition_params.get('taskDefinition').get('timeout')),
            'delayTime': int(process_definition_params.get('taskDefinition').get('delayTime')),
            'cpuQuota': -1,
            'memoryMax': -1,
            }
        ]
    other_params = ''
    execution_type = "PARALLEL"
    
    params = {
        'name': name,
        'description': description,
        'globalParams': global_params,
        'locations': locations,
        'timeout': timeout,
        'taskRelationJson': json.dumps(task_relation),
        'taskDefinitionJson': json.dumps(task_definition),
        'otherParamsJson': json.dumps(other_params),
        'executionType': execution_type,
    }
    
    try:
        response = requests.post(url, headers=headers, params=params)
        response.raise_for_status()
        json_data = response.json()
    except Exception as e:
        print(f'Request failed, error: {e}. at {sys._getframe().f_lineno} in {sys._getframe().f_code.co_name}')
        sys.exit(1)

    success = json_data.get('success')
    failed = json_data.get('failed')
    if (not success) or failed:
        code = json_data.get('code')
        msg = json_data.get('msg')
        print(f'Create failed, code: {code}, msg: {msg}')
        sys.exit(1)
    
    data = json_data.get('data')
    print(data)
    
    