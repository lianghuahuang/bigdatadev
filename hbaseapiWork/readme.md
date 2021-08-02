### 作业：编程实践，使用Java API操作HBase
- 实践主要是建表，插入数据，删除数据，查询等功能。建立一个如下所示的表：
- 表名：$your_name:student
- 空白处自行填写, 姓名学号一律填写真实姓名和学号
![image](https://user-images.githubusercontent.com/8264550/127257905-9e8882f4-32e0-452b-b61b-55eeeb57c321.png)
- 服务器版本为2.1.0（hbase版本和服务器上的版本可以不一致，但尽量保证一致）

程序执行结果截图
![image](https://user-images.githubusercontent.com/8264550/127820228-2849eb29-8742-4d2d-aae9-85a6bd379f02.png)


### 需要注意的几个问题：
- windows环境需要下载hadoop对应版本文件并配置环境变量 HADOOP_HOME  ,path配置 bin和sbin，以及winutils环境准备
- hosts文件配置，因访问线上环境，集群调用时用hostname访问而非ip，因此需要配置相关节点的host映射关系


