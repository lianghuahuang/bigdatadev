## 题目1：
### 简单：展示电影ID为2116这部电影各年龄段的平均影评分
select u.age,avg(tr.rate) as avg_rate from  
hive_sql_test1.t_movie as m 
join hive_sql_test1.t_rating as tr 
on m.movieid=tr.movieid 
join hive_sql_test1.t_user as u 
on u.userid=tr.userid  
where m.movieid=2116
group by u.age

## 题目2：
### 中等：找出男性评分最高且评分次数超过50次的10部电影，展示电影名，平均影评分和评分次数
### 题目未要求显示sex字段，因为要求是男性评分，但是题目截图有sex字段，因此增加了group by sex字段
select u.sex,m.moviename as name,avg(tr.rate) as avg_rate,count(tr.rate) as total from  
hive_sql_test1.t_movie as m 
join hive_sql_test1.t_rating as tr 
on m.movieid=tr.movieid 
join hive_sql_test1.t_user as u 
on u.userid=tr.userid  
where u.sex='M'
group by m.movieid,m.moviename,u.sex
having count(tr.rate)>50
order by avg_rate desc
limit  10

## 题目3
### 困难：找出影评次数最多的女士所给出最高分的10部电影的平均影评分，展示电影名和平均影评分（可使用多行SQL）
### 影评次数最多的女士给出最高分5分的电影有59部，导致limit 10的结果和截图不一致
select m.moviename,avg(tr.rate) as avg_rate
from hive_sql_test1.t_movie as m 
join hive_sql_test1.t_rating as tr 
on m.movieid=tr.movieid 
join (
select tr.movieid,tr.rate from  
hive_sql_test1.t_rating  tr
join (
select  u1.userid,count(u1.userid) c from  
hive_sql_test1.t_rating as tr1 
join hive_sql_test1.t_user as u1
on u1.userid=tr1.userid  
where u1.sex='F'
group by u1.userid
order by c desc
limit 1) u
on u.userid = tr.userid
order by tr.rate desc
limit 10) tr2
on tr2.movieid=m.movieid
group by m.movieid,m.moviename 

## 附加作业：GeekFileFormat
### 请优先完成前面三个作业，Hive的练习更多的是对于HQL的使用，对于完成的同学，可以试着写一个Hive的FileFormat：GeekFileFormat
### 要求：
- 实现两个类：GeekTextInputFormat和GeekTextOutputFormat
- 建表时使用create table ... stored as geek来创建GeekFormat表
- 该表的文件类型为文本类型，非二进制类型
- 类似Base64TextInputFormat和Base64TextOutputFormat，GeekFormat也是用于加密
- 解密规则如下：文件中出现任何的geek，geeeek，geeeeeeeeeeek等单词时，进行过滤，即删除该单词。gek需要保留。字母中连续的“e”最大长度为256个。
  - 例如：    This notebook can be geeeek used to geek install gek on all geeeek worker nodes, run data generation, and create the TPCDS geeeeeeeeek database.
  - 解密为：This notebook can be used to install gek on all worker nodes, run data generation, and create the TPCDS database.
- 【附加的附加】加密规则如下：文件输出时每随机2到256个单词，就插入一个gee...k，字母e的个数等于前面出现的非gee...k单词的个数。
  - 例如：    This notebook can be used to install gek on all worker nodes, run data generation, and create the TPCDS database.
  - 加密为：This notebook can be geeeek used to geek install gek on all geeeek worker nodes, run data generation, and create the TPCDS geeeeeeeeek database.

