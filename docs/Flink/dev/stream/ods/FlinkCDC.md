# FlinkCDC
### 代码概述
> 此代码的主要功能是从 MySQL 数据库中捕获数据变更，并将这些变更数据以 JSON 格式发送到 Kafka 主题。它运用了 Apache Flink 框架，借助 Debezium 来捕获 MySQL 的二进制日志（binlog），从而实现增量数据的捕获。

### 注意事项
> 1.要确保 MySQL 服务器开启了二进制日志（binlog）功能，因为 Debezium 是通过捕获 binlog 来获取数据变更的。 \
> 2.若需要更高的性能和容错能力，可以调整并行度和检查点间隔。 \
> 3.要保证 Kafka 主题已经创建，否则数据无法正常发送。