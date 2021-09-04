## 1. 为Spark SQL添加一条自定义命令
• SHOW VERSION;
• 显示当前Spark版本和Java版本
### SqlBase.g4文件添加SHOW VERSION语法
statement
![image](https://user-images.githubusercontent.com/8264550/132078660-ccfa0f78-86c3-4aa4-8915-3c0f595d2a88.png)
ansiNoReserved 和 noReserved 添加 "| VERSION"
keyword list start 添加 "VERSION: 'VERSION'"
### SparkSqlParser.scala 添加visitShowVersion方法
``` 
  override def visitShowVersion(ctx: ShowVersionContext): LogicalPlan = withOrigin(ctx) {
      ShowMyVersionCommand()
  }
``` 
### 新增 ShowMyVersionCommand.scala
```
case class ShowMyVersionCommand() extends RunnableCommand{

  override val output: Seq[Attribute] =
    Seq(AttributeReference("version", StringType, nullable = true)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val javaVersion = System.getProperty("java.version")
    val sparkVersion = sparkSession.version;
    val outputString = "java version:" + javaVersion + " spark version:" + sparkVersion
    Seq(Row(outputString))
  }
}

```
### 编译打包后执行结果：
![image](https://user-images.githubusercontent.com/8264550/132078588-1ea0bf50-aaa3-46d6-9586-c442d3681da8.png)

## 2. 构建SQL满足如下要求
### 通过set spark.sql.planChangeLog.level=WARN;查看
### 1. 构建一条SQL，同时apply下面三条优化规则：
- CombineFilters
- CollapseProject
- BooleanSimplification
### SQL语句如下：1=1应用了BooleanSimplification司马，子查询语句应用collaseProject ，子查询和主查询条件应用了 combineFilters
select productName from (select customerId,productName from sales where 1=1 and  customerId not in('a','b','c'))  a where customerId='d';   
### 2. 构建一条SQL，同时apply下面五条优化规则：
- ConstantFolding
- PushDownPredicates
- ReplaceDistinctWithAggregate
- ReplaceExceptWithAntiJoin
- FoldablePropagation
### SQL语句如下：except语句应用了ReplaceExceptWithAntiJoin，distinct应用了ReplaceDistinctWithAggregate，“123 AS number” 作为常量传导到了后面的order by所以应用了FoldablePropagation，“amountPaid<10+2 ” 中的10+2应用了ConstantFolding，同时该条件语句应用了PushDownPredicates将条件应用到了子查询语句（截图为部分优化规则）
select DISTINCT customerId,productName from (select customerId,productName,amountPaid,123 AS number from sales order by number) a where amountPaid<10+2  except DISTINCT select customerId,productName from sales where amountPaid>5;
![image](https://user-images.githubusercontent.com/8264550/132088989-f0cdc23a-60ca-4407-9da0-ba9469c50e46.png)
![image](https://user-images.githubusercontent.com/8264550/132089011-92a5e00e-7474-4e69-b81a-316399510a42.png)

