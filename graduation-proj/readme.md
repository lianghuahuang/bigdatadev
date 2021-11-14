### 题目一： 分析一条 TPCDS SQL（请基于 Spark 3.1.1 版本解答） 
SQL 从中任意选择一条：
https://github.com/apache/spark/tree/master/sql/core/src/test/resources/tpcds
- （1）运行该 SQL，如 q38，并截图该 SQL 的 SQL 执行图(q63)
![image](https://user-images.githubusercontent.com/8264550/141675009-3eb9e083-cc87-4736-b6be-0bcfa793f2cc.png)

- （2）该 SQL 用到了哪些优化规则（optimizer rules） 
  - ColumnPruning 列剪枝
  - ReorderJoin join过滤操作下推
  - PushDownPredicates 谓词下推
  - CollapseProject 投影算子消除
  - NullPropagation 空传播
  - ConstantFolding 常量折叠
  - SimplifyCasts 简化转换
  - OptimizeIn 关键字in优化，替代为InSet
  - InferFiltersFromConstraints 对join的过滤操作进行优化
  - DecimalAggregates
  - RewritePredicateSubquery 将特定子查询为此逻辑转换为left-semi/anti join
- （3）请各用不少于 200 字描述其中的两条优化规则
  1、ColumnPruning 列剪枝：尝试从逻辑计划中去掉不需要的列，从而减少读取数据的量。该规则会在多种情况下生效：  
  - 当有groupBy等聚合操作时，会把不需要的列在读取数据时去掉，以减少数据的读取量；
  - 在Union+select的组合去掉不需要的属性；  
  - 在进行Join+select时去掉不需要的列；  
  - 在进行window+select操作时，去掉没有对其进行操作的列
  在q63中只需要读取i_manager_id,ss_sales_price
  2、ReorderJoin：对Join操作进行重新排列，把所有的过滤条件推入join中，以便保证join内部至少有一个过滤条件
帮助文档：如何运行该 SQL：
1. 从 github 下载 TPCDS 数据生成器
>git clone https://github.com/maropu/spark-tpcds-datagen.git
>cd spark-tpcds-datagen
2. 下载 Spark3.1.1 到 spark-tpcds-datagen 目录并解压
>wget https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz
>tar -zxvf spark-3.1.1-bin-hadoop2.7.tgz
3. 生成数据
>mkdir -p tpcds-data-1g
>export SPARK_HOME=./spark-3.1.1-bin-hadoop2.7
>./bin/dsdgen --output-location tpcds-data-1g
4. 下载三个 test jar 并放到当前目录
>wget 
https://repo1.maven.org/maven2/org/apache/spark/spark-catalyst_2.12/3.1.1/spark-catalyst_2.1
2-3.1.1-tests.jar
>wget 
https://repo1.maven.org/maven2/org/apache/spark/spark-core_2.12/3.1.1/spark-core_2.12-3.1.1-
tests.jar
>wget 
https://repo1.maven.org/maven2/org/apache/spark/spark-sql_2.12/3.1.1/spark-sql_2.12-3.1.1-te
sts.jar
5. 执行 SQL
>./spark-3.1.1-bin-hadoop2.7/bin/spark-submit --class 
org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark --jars 
spark-core_2.12-3.1.1-tests.jar,spark-catalyst_2.12-3.1.1-tests.jar 
spark-sql_2.12-3.1.1-tests.jar --data-location tpcds-data-1g --query-filter "q73"

### 题目二：架构设计题 
你是某互联网公司的大数据平台架构师，请设计一套基于 Lambda 架构的数据平台架构，要求尽可能多的把课程中涉及的组件添加到该架构图中。 
并描述 Lambda 架构的优缺点，要求不少于 300 字。

### 题目三：简答题（三选一） 
- A：简述 HDFS 的读写流程，要求不少于 300 字 
- B：简述 Spark Shuffle 的工作原理，要求不少于 300 字   
 Spark在DAG调度阶段会将一个Job划分为多个Stage，上游Stage做map工作，下游Stage做reduce工作，其本质上还是MapReduce计算框架。  
 Shuffle是连接map和reduce之间的桥梁，它将map的输出对应到reduce输入中，这期间涉及到序列化反序列化、跨节点网络IO以及磁盘读写IO等.  
 Spark在DAG阶段以shuffle为界，划分stage，上游stage做map task，每个map task将计算结果数据分成多份，每一份对应到下游stage的每个partition中，并将其临时写到磁盘，该过程叫做shuffle write；  
 下游stage做reduce task，每个reduce task通过网络拉取（fetch）上游stage中所有map task的指定分区结果数据，该过程叫做shuffle read，最后完成reduce的业务逻辑
- C：简述 Flink SQL 的工作原理，要求不少于 300 字

