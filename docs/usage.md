# 使用文档

[TOC]

<br>

## 名词解释

| 名词 | 英文 | 解释 |
| --- | --- | --- |
| 租户 | Tenant | 租户对应的是Linux（部署机器的操作系统）的用户，用于worker（集群工作节点）提交作业所使用的用户。 |
| 用户 | User | 系统使用者，用户分为管理员用户和普通用户。 |
| 告警插件实例 | Alert Plugin Instance | 选择告警插件（Email、Telegram、DingTalk、WeChat、Webex Teams、Script、Http、Feishu、Slack等）创建告警实例。<br>在创建告警实例时，需要选择告警策略（成功发、失败发、成功和失败都发）。<br>创建告警组时，需要选择相应的告警实例。 |
| 告警组 | Alert Group | 告警组是在启动时设置的参数，在流程结束以后会将流程的状态和其他信息以邮件形式发送给告警组。 |
| 令牌 | Token | 令牌管理提供了一种可以通过调用接口的方式对系统进行各种操作。 |
| 数据源 | Data Source | 支持MySQL、POSTGRESQL、HIVE/IMPALA、SPARK、CLICKHOUSE、ORACLE、SQLSERVER等数据源。 |
| 项目 | Project | 用户的项目，项目包含工作流（流程）定义、工作流实例、工作流的任务节点。 |
| 工作流（流程） | Workflow（Process）| 为项目创建的工作流程。 |
| 任务 | Task | 工作流中的节点任务。 |

<br>

## 功能介绍（部分）

### 数据质量

> [数据质量概述](https://dolphinscheduler.apache.org/zh-cn/docs/3.2.2/guide/data-quality)

#### 执行逻辑

```mermaid
    sequenceDiagram
        actor user as 用户
        participant api as UI and API Server
        participant master as Master Server
        participant worker as Worker Server
        participant alert as Alert Server
        participant spark as Spark
        participant hdfs as HDFS
        participant db as DB

        user ->> api : 定义任务
        activate api
        api ->> db : 保存TaskParam
        deactivate api

        master ->> db : 获取TaskParam
        activate db
        db -->> master : TaskParam
        deactivate db

        activate master
        master ->> master : 启动运行任务
        activate master
        master ->> worker : 解析TaskParam，封装DataQualityTask所需要的参数下发
        activate worker
        worker ->> worker : 运行数据质量任务
        worker ->> spark : 运行数据质量任务
        activate spark
        spark -> hdfs : 读写数据
        spark -->> worker : 数据质量任务运行结束
        deactivate spark
        worker ->> db : 将统计结果写入到指定的存储引擎中<br>当前数据质量任务结果存储在dolphinscheduler的t_ds_dq_execute_result表中
        worker ->> master : 发送任务结果TaskResponse
        deactivate worker
        alt 任务类型为DataQualityTask
            master ->> master : 根据taskInstanceId从t_ds_dq_execute_result中读取相应的结果<br>根据用户配置好的检查方式，操作符和阈值进行结果判断
            alt 结果失败
                alt 用户配置的失败策略为告警
                    master ->> alert : 告警
                else 用户配置的失败策略为中断
                    master ->> master : 中断
                end
            end
            deactivate master
        end
        deactivate master
```

<br>

+ > 用户在界面定义任务，用户输入值保存在TaskParam中 运行任务时，Master会解析TaskParam，封装DataQualityTask所需要的参数下发至Worker。 Worker运行数据质量任务，数据质量任务在运行结束之后将统计结果写入到指定的存储引擎中，当前数据质量任务结果存储在dolphinscheduler的t_ds_dq_execute_result表中 Worker发送任务结果给Master，Master收到TaskResponse之后会判断任务类型是否为DataQualityTask，如果是的话会根据taskInstanceId从t_ds_dq_execute_result中读取相应的结果，然后根据用户配置好的检查方式，操作符和阈值进行结果判断，如果结果为失败的话，会根据用户配置好的的失败策略进行相应的操作，告警或者中断

<br>

#### 检查逻辑

+ ```java

    // 校验方式
    public enum CheckFormula {
        ExpectedMinusActual {
            // Expected - Actual
            public Number data(Number expected, Number actual) {
                return expected - actual;
            }
        },
        ActualMinusExpected {
            // Actual - Expected
            public Number data(Number expected, Number actual) {
                return actual - expected;
            }
        },
        ActualDividedByExpectedPercentage {
            // (Actual / Expected) x 100%
            public Number data(Number expected, Number actual) {
                Number ratio = actual / expected;
                return ratio * 100;
            }
        },
        DifferenceRatioPercentage {
            // (Expected - Actual) / Expected x 100%
            public Number data(Number expected, Number actual) {
                Number difference = expected - actual;
                Number ratio = difference / expected;
                return ratio * 100;
            }
        };

        public abstract Number data(Number expected, Number actual);
    }

    // 操作符
    public enum Operator {
        EQUAL {
            // 等于操作符
            public boolean compare(Number x, Number y) { return x == y; }
        },
        GREATER_THAN {
            // 大于操作符
            public boolean compare(Number x, Number y) { return x > y; }
        },
        GREATER_EQUAL {
            // 大于等于操作符
            public boolean compare(Number x, Number y) { return x >= y; }
        },
        LESS_THAN {
            // 小于操作符
            public boolean compare(Number x, Number y) { return x < y; }
        },
        LESS_EQUAL {
            // 小于等于操作符
            public boolean compare(Number x, Number y) { return x <= y; }
        },
        NOT_EQUAL {
            // 不等于操作符
            public boolean compare(Number x, Number y) { return x != y; }
        };

        public abstract boolean compare(Number x, Number y);
    }

    // 期望值
    public class ExpectedValue {
        private ExpectedValueType type;

        public ExpectedValue(ExpectedValueType type) {
            this.type = type;
        }

        public ExpectedValueType getValueType() {
            return type;
        }

        public Number getValue() {
            Number value;
            switch (this.type) {
                case ExpectedValueType.FixValue:
                    value = SomeClass.getFixValue();
                    break;
                case ExpectedValueType.DailyAvg:
                    value = SomeClass.getDailyAvg();
                    break;
                case ExpectedValueType.WeeklyAvg:
                    value = SomeClass.getWeeklyAvg();
                    break;
                case ExpectedValueType.MonthlyAvg:
                    value = SomeClass.getMonthlyAvg();
                    break;
                case ExpectedValueType.Last7DayAvg:
                    value = SomeClass.getLast7DayAvg();
                    break;
                case ExpectedValueType.Last30DayAvg:
                    value = SomeClass.getLast30DayAvg();
                    break;
                case ExpectedValueType.SrcTableTotalRows:
                    value = SomeClass.getSrcTableTotalRows();
                    break;
                case ExpectedValueType.argetTableTotalRows:
                    value = SomeClass.getTargetTableTotalRows();
                    break;
                default:
                    throw new InvalidArgumentException("Invalid expected value type");
            }

            return value;
        }

        // 期望值类型
        public enum ExpectedValueType {
            FixValue,               // 固定值
            DailyAvg,               // 日均值
            WeeklyAvg,              // 周均值
            MonthlyAvg,             // 月均值
            Last7DayAvg,            // 最近7天均值
            Last30DayAvg,           // 最近30天均值
            SrcTableTotalRows,      // 源表总行数
            TargetTableTotalRows    // 目标表总行数
        }
    }

    // 校验
    public class Check {
        CheckFormula checkFormula;
        Operator operator;
        ExpectedValue expectedValue;
        Number threshold;   // 阈值

        public Check(CheckFormula checkFormula, Operator operator, ExpectedValue expectedValue, Number threshold) {
            this.checkFormula = checkFormula;
            this.operator = operator;
            this.expectedValue = expectedValue;
            this.threshold = threshold;
        }

        // 校验公式
        public method(Number actualValue /* 实际值 */) {
            Number data;

            // 校验方式数据
            data = this.checkFormula.data(expectedValue.getValue(), actualValue);
            // [校验方式][操作符][阈值]
            if (operator.compare(data, this.threshold)) {
                // 结果为真，则表明数据不符合期望，执行失败策略
                SomeClass.executeFailureStrategy();
            }
        }
    }

```