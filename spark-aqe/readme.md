## 1.思考题：如何避免小文件问题
• 如何避免小文件问题？给出2～3种解决方案
### coalesce和repartition 减小分区数
### 通过AEQ来动态缩减partition数量
### 参考Iceberg 压缩合并小文件

## 2. 实现Compact table command
• 要求：
添加compact table命令，用于合并小文件，例如表test1总共有50000个文件，
每个1MB，通过该命令，合成为500个文件，每个约100MB。 • 语法：
COMPACT TABLE table_identify [partitionSpec] [INTO fileNum FILES];
• 说明：
1.如果添加partitionSpec，则只合并指定的partition目录的文件。
2.如果不加into fileNum files，则把表中的文件合并成128MB大小。
3.以上两个算附加要求，基本要求只需要完成以下功能：
COMPACT TABLE test1 INTO 500 FILES;
代码参考
SqlBase.g4:
| COMPACT TABLE target=tableIdentifier partitionSpec?
(INTO fileNum=INTEGER_VALUE identifier)? #compactTable
sqlbase.g4定义
![image](https://user-images.githubusercontent.com/8264550/134325833-f961cf88-c24a-4011-ac6f-bfbb9b087957.png)

CompactTableCommand.scala (未完成1、2的要求)
```
case class CompactTableCommand(tableIdent: TableIdentifier,
                               partitionSpec: Map[String, Option[String]],
                               fileNum: Integer) extends RunnableCommand{
  private def getPartitionSpec(table: CatalogTable): Option[TablePartitionSpec] = {
    val normalizedPartitionSpec =
      PartitioningUtils.normalizePartitionSpec(partitionSpec, table.partitionSchema,
        table.identifier.quotedString, conf.resolver)

    // Report an error if partition columns in partition specification do not form
    // a prefix of the list of partition columns defined in the table schema
    val isNotSpecified =
    table.partitionColumnNames.map(normalizedPartitionSpec.getOrElse(_, None).isEmpty)
    if (isNotSpecified.init.zip(isNotSpecified.tail).contains((true, false))) {
      val tableId = table.identifier
      val schemaColumns = table.partitionColumnNames.mkString(",")
      val specColumns = normalizedPartitionSpec.keys.mkString(",")
      throw new AnalysisException("The list of partition columns with values " +
        s"in partition specification for table '${tableId.table}' " +
        s"in database '${tableId.database.get}' is not a prefix of the list of " +
        "partition columns defined in the table schema. " +
        s"Expected a prefix of [${schemaColumns}], but got [${specColumns}].")
    }

    val filteredSpec = normalizedPartitionSpec.filter(_._2.isDefined).mapValues(_.get)
    if (filteredSpec.isEmpty) {
      None
    } else {
      Some(filteredSpec.toMap)
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sessionState = sparkSession.sessionState
    val db = tableIdent.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = TableIdentifier(tableIdent.table, Some(db))
    val tableMeta = sessionState.catalog.getTableMetadata(tableIdentWithDB)
    if (tableMeta.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException("COMPACT TABLE is not supported on views.")
    }

    val partitionValueSpec = getPartitionSpec(tableMeta)
    if(partitionValueSpec!=null) {
      val partitions = sessionState.catalog.listPartitions(tableMeta.identifier, partitionValueSpec)
      if (partitions.isEmpty) {
        if (partitionValueSpec.isDefined) {
          throw new NoSuchPartitionException(db, tableIdent.table, partitionValueSpec.get)
        } else {
          // the user requested to analyze all partitions for a table which has no partitions
          // return normally, since there is nothing to do
          return Seq.empty[Row]
        }
      } else if (partitions.size < fileNum) {
        return Seq.empty[Row]
      }
    }

    sparkSession.read.table(tableIdent.identifier).repartition(fileNum);
    sparkSession.catalog.refreshTable(tableMeta.identifier.quotedString)
    CommandUtils.updateTableStats(sparkSession, tableMeta)
    Seq.empty[Row]
  }
}
```

SparkSqlParse.scala visitCompactTable方法
```
  override def visitCompactTable(ctx:CompactTableContext): LogicalPlan = withOrigin(ctx) {
     CompactTableCommand(visitTableIdentifier(ctx.target), visitPartitionSpec(ctx.partitionSpec()),
       ctx.fileNum.getText.toInt)
  }
```
