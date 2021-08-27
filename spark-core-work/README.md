## 作业一使用RDD API实现带词频的倒排索引
### 倒排索引（Inverted index），也被称为反向索引。它是文档检索系统中最常用的数据结构。被广泛地应用于全文搜索引擎。例子如下，被索引的文件为（0，1，2代表文件名）
- 0. "it is what it is"
- 1. "what is it"
- 2. "it is a banana"
### 我们就能得到下面的反向文件索引：
- "a": {2}
- "banana": {2}
- "is": {0, 1, 2}
- "it": {0, 1, 2}
- "what": {0, 1}
### 再加上词频为：
- "a": {(2,1)}
- "banana": {(2,1)}
- "is": {(0,2), (1,1), (2,1)}
- "it": {(0,2), (1,1), (2,1)}
- "what": {(0,1), (1,1)}
## 代码：FileWordCount.scala

## 作业二Distcp的spark实现
### 使用Spark实现Hadoop 分布式数据传输工具DistCp (distributed copy)，只要求实现最基础的copy功能，对于-update、-diff、-p不做要求
### 对于HadoopDistCp的功能与实现，可以参考
https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html
https://github.com/apache/hadoop/tree/release-2.7.1/hadoop-tools/hadoop-distcp
Hadoop使用MapReduce框架来实现分布式copy，在Spark中应使用RDD来实现分布式copy
### 应实现的功能为：
sparkDistCp hdfs://xxx/source hdfs://xxx/target
### 得到的结果为，启动多个task/executor，将hdfs://xxx/source目录复制到hdfs://xxx/target，得到
hdfs://xxx/target/source
### 需要支持source下存在多级子目录
### 需支持-i Ignore failures 参数
### 需支持-m max concurrence参数，控制同时copy的最大并发task数
## 代码：SparkDistCP.scala 完成了多级目录和最大并发task数支持，采用scopt来进行命令行参数解析。未完成 ignore failures
