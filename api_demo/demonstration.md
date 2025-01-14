# API DEMO

<style>
  h1 {
    counter-reset: h2
  }
  h2 {
    counter-reset: h3
  }
  h3 {
    counter-reset: h4
  }
  h2:before {
    counter-increment: h2;
    content: counter(h2) ". "
  }
  h3:before {
    counter-increment: h3;
    content: counter(h2) "." counter(h3) ". "
  }
  h4:before {
    counter-increment: h4;
    content: counter(h2) "." counter(h3) "." counter(h4) ". "
  }
</style>

[TOC]

<br>

## 数据源

+ | file | summary | version |
  | --- | --- | -- |
  | query_datasource_list_by_database_type.py | 通过数据源类型查询数据源列表 | v1 |
  | query_datasource_list_paging.py | 分页查询数据源列表 | v1 |
  | query_datasource_by_id.py | 查询数据源通过ID | v1 |
  | create_datasource.py | 创建数据源 | v1 |
  | update_datasource.py | 更新数据源 | v1 |
  | delete_datasource.py | 删除数据源 | v1 |
  | connect_datasource_test.py | 连接数据源测试 | v1 |
  | get_datasource_database.py | 获取数据源库列表 | v1 |
  | get_datasource_table.py | 获取数据源表列表 | v1 |
  | get_datasource_table_columns.py | 获取数据源表列名 | v1 |

<br>

## 项目

+ | file | summary | version |
  | --- | --- | -- |
  | query_all_project_list.py | 查询所有项目 | v1 |
  | query_authorized_and_user_created_project.py | 查询授权和用户创建的项目 | v1 |
  | query_project_info_by_project_code.py | 通过项目代码查询项目信息 | v1 |
  | create_project.py | 创建项目 | v1 |
  | update_project.py | 更新项目 | v1 |
  | delete_project_by_code.py | 通过代码删除项目 | v1 |
  | v2_query_all_project_list.py | 查询所有项目 | v2 |
  | v2_query_authorized_and_user_created_project.py | 查询授权和用户创建的项目 | v2 |
  | v2_query_project_info_by_project_code.py | 通过项目代码查询项目信息 | v2 |
  | v2_create_project.py | 创建项目 | v2 |
  | v2_update_project.py | 更新项目 | v2 |
  | v2_delete_project_by_code.py | 通过代码删除项目 | v2 |

<br>

## 工作流

### 工作流定义

+ | file | summary | version |
  | --- | --- | -- |
  | query_process_definition_list.py | 查询流程定义列表 | v1 |
  | query_process_definition_simple_list | 查询流程定义简单列表 | v1 |
  | query_process_definition_list_by_project_code.py | 通过项目代码查询流程定义列表 | v1 |
  | query_all_process_definition_by_project_code.py | 通过项目代码查询所有流程定义 | v1 |
  | query_process_definition_by_code.py | 通过流程定义代码查询流程定义 | v1 |
  | query_process_definition_by_name.py | 通过流程定义名字查询流程定义 | v1 |
  | create_process_definition.py | 创建流程定义 | v1 |
  | update_process_definition.py | 更新流程定义 | v1 |
  | delete_process_definition_by_code.py | 通过代码删除流程定义 | v1 |
  | verify_process_definition_name.py | 验证流程定义名字 | v1 |

<br>

#### 数据质量任务

+ | file | summary | version |
  | --- | --- | -- |
  | task-data-quality/create_process_definition_for_null_value_check.py | 创建有空值检查任务的流程定义 | v1 |
  | task-data-quality/update_process_definition_for_null_value_check.py | 更新有空值检查任务的流程定义 | v1 |

<br>

### 工作流实例

+ | file | summary | version |
  | --- | --- | -- |
  | query_process_instance_list.py | 查询流程实例列表 | v1 |

<br>

### 工作流定时

+ | file | summary | version |
  | --- | --- | -- |
  | query_schedule_list.py | 查询定时列表 | v1 |
  | create_schedule.py | 创建定时 | v1 |
  | update_schedule.py | 更新定时 | v1 |
  | delete_schedule_by_id.py | 根据定时id删除定时数据 | v1 |
  | online_schedule.py | 定时上线 | v1 |
  | offline_schedule.py | 定时下线 | v1 |

<br>

## 数据质量

+ | file | summary | version |
  | --- | --- | -- |
  | get_data_quality_execute_result_by_process_instance_id.py | 根据流程实例id获取数据质量任务执行结果 |

<br>