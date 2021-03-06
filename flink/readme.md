## 作业题
report(transactions).executeInsert("spend_report");  

将transactions表经过report函数处理后写入到spend_report表。  

每分钟（小时）计算在五分钟（小时）内每个账号的平均交易金额（滑动窗口）？  

注：使用分钟还是小时作为单位均可


### SpendReport.java代码
```
import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.*;

public class SpendReport {

    public static Table report(Table transactions) {
        return transactions
                //滑动窗口，固定长度为5分钟，滑动间隔1分钟
                .window(Slide.over(lit(5).minute()).every(lit(1).minute()).on($("transaction_time")).as("log_ts"))
                .groupBy($("account_id"), $("log_ts"))
                .select(
                        $("account_id"),
                        $("log_ts").start().as("log_ts"),
                        $("amount").avg().as("amount"));

    }

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql("CREATE TABLE transactions (\n" +
                "    account_id  BIGINT,\n" +
                "    amount      BIGINT,\n" +
                "    transaction_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'transactions',\n" +
                "    'properties.bootstrap.servers' = 'kafka:9092',\n" +
                "    'format'    = 'csv'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE spend_report (\n" +
                "    account_id BIGINT,\n" +
                "    log_ts     TIMESTAMP(3),\n" +
                "    amount     BIGINT\n," +
                "    PRIMARY KEY (account_id, log_ts) NOT ENFORCED" +
                ") WITH (\n" +
                "  'connector'  = 'jdbc',\n" +
                "  'url'        = 'jdbc:mysql://mysql:3306/sql-demo',\n" +
                "  'table-name' = 'spend_report',\n" +
                "  'driver'     = 'com.mysql.jdbc.Driver',\n" +
                "  'username'   = 'sql-demo',\n" +
                "  'password'   = 'demo-sql'\n" +
                ")");

        Table transactions = tEnv.from("transactions");
        report(transactions).executeInsert("spend_report");
    }
}
```
### 程序执行结果
![image](https://user-images.githubusercontent.com/8264550/138648743-8054b5d2-55ee-456b-a7cb-dd6e0085cdfd.png)
![image](https://user-images.githubusercontent.com/8264550/138648800-e5411679-b51c-478a-8f93-664458baab6e.png)
![image](https://user-images.githubusercontent.com/8264550/138651131-7f96831a-fd24-4524-b8b5-b5b04cd26428.png)



