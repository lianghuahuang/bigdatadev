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
