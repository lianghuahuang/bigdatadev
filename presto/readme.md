## HyperLogLog算法在Presto的应用
### 1. 搜索HyperLogLog算法相关内容，了解其原理，写出5条HyperLogLog的用途或大数据场景下的实际案例。
在大数据场景下，在可接受的误差范围内，找出不重复的基数数量，降低内存开销。比如：
- 统计注册 IP 数
- 统计每日访问 IP 数
- 统计页面实时 UV 数
- 统计在线用户数
- 统计用户每天搜索不同词条的个数
- 流量监控（多少不同IP访问过一个服务器）
- 数据库查询优化（例如我们是否需要排序和合并，或者是否需要构建哈希表）
### 2. 在本地docker环境或阿里云e-mapreduce环境进行SQL查询，要求在Presto中使用HyperLogLog计算近似基数。（请自行创建表并插入若干数据）
#### 本地docker环境，安装 alluxio-presto-sandbox镜像，use alluxio schema然后执行如下语句，结果显示HLL函数结果存在一定误差，但是每秒扫描的记录数要快很多
 select count(distinct ss_customer_sk) from store_sales;
 select approx_distinct(ss_customer_sk) from store_sales;
![image](https://user-images.githubusercontent.com/8264550/134851557-3062045b-0f20-437b-b6ac-4cf3fc4b963d.png)

### 3. 学习使用Presto-Jdbc库连接docker或e-mapreduce环境，重复上述查询。（选做）
maven 依赖
``` 
        <dependency>
            <groupId>com.facebook.presto</groupId>
            <artifactId>presto-jdbc</artifactId>
            <version>0.262</version>
        </dependency>
``` 
PrestoJdbcSample.java
``` 
import java.sql.*;

public class PrestoJdbcSample {  
   public static void main(String[] args) {  
      Connection connection = null; 
      Statement statement = null;  
      try { 
         
         Class.forName("com.facebook.presto.jdbc.PrestoDriver");
         connection = DriverManager.getConnection(
         "jdbc:presto://localhost:8080/hive/alluxio", "root", "");
         
         //connect mysql server tutorials database here 
         statement = connection.createStatement(); 
         String sql;  
         sql = " select count(distinct ss_customer_sk) customerCount from store_sales";
        
         //select mysql table author table two columns  
         ResultSet resultSet = statement.executeQuery(sql);  
         while(resultSet.next()){  
            int customerCount  = resultSet.getInt("customerCount");
            System.out.println("customerCount: " + customerCount);
         }
         sql= "  select approx_distinct(ss_customer_sk) as customerCount from store_sales";
         resultSet = statement.executeQuery(sql);
         while(resultSet.next()){
            int customerCount  = resultSet.getInt("customerCount");
            System.out.println("hll customerCount: " + customerCount);
         }

         resultSet.close(); 
         statement.close(); 
         connection.close(); 
         
      }catch(SQLException sqlException){ 
         sqlException.printStackTrace(); 
      }catch(Exception exception){ 
         exception.printStackTrace(); 
      } 
   } 
}
``` 
程序执行结果如下：
![image](https://user-images.githubusercontent.com/8264550/134856505-3a6f9e84-de47-495e-84d9-8b34ce23ab97.png)
