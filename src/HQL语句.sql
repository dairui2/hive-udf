-- 参考：
    -- http://blog.csdn.net/hguisu/article/details/7256833 
    -- http://www.cnblogs.com/tangtianfly/archive/2012/03/13/2393449.html
    -- http://bupt04406.iteye.com/blog/1097437

-- create语句,创建内部表
    create table autolog_1(time int,ip string,addr string,suv string,domain string,url string) 
    partitioned by(pt int) 
    clustered by(domain) sorted by(time) into 100 buckets
    row format delimited fields terminated by '\t' lines terminated by '\n'
    stored as textfile/sequencefile;
    -- stored as 可以指定类型，如lzo文件 ：STORED AS INPUTFORMAT "com.hadoop.mapred.DeprecatedLzoTextInputFormat" 
       OUTPUTFORMAT "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
-- create语句，创建外部表
    create external table student(id int,name string,age int,addr string) 
    row format delimited fields terminated by '\t' 
    location 'hdfs:///user/hadoop/hongxingfan/student/';
    -- 总结：
        -- 在hive中还有array，map，struct等符合数据类型
        -- 在参考网址上有更全的create语句
        -- 重点理解partition，cluster，sort等常用且证明有效率的用法
        -- 注意内部表与外部表的区别
    
-- 改变表
    -- alter table autolog rename to autolog_1;
    -- alter table autolog change age age1 int first/(after name);
    -- alter table autolog add/replace columns age int;
    -- alter table autolog set fileformat textfile; 
    -- alter table autolog clustered by(domain) sorted by(time) into 32 buckets;
    -- 总结：
        -- 该变表的语句用到的时候不会很多，cluster修改后对之前的数据不会有影响，对后续数据起作用
        
-- 分区操作
    -- alter table add partition(pt=20141020) location '/home/hongxingfan/20141020/part0000'
    -- alter table autolog drop partition(pt=20141020,domain='auto.sohu.com')
    
-- 展示描述语句
    -- show tables '*auto*';
    -- show partitions autolog;
    -- show table extended like "*auto*";
    -- show table extended like autolog partition(pt=20141020);
    -- 这几个语句应用比较广泛，使用频率比较高
    
-- desc描述语句
    -- desc function length;
    -- desc extended autolog;
    -- desc extended autolog partition(pt=20141020);
    -- desc formatted autolog; --比extended更友好
    -- desc formatted autolog partition(pt=20141020);
    -- 总结：
        -- show主要是显示，desc是描述详情
        
-- load语句
    -- load data local inpath './autolog-20141019' overwrite into table autolog_1 partition(pt='20141019') ;
    
-- insert语句
    -- insert overwrite table autolog_1 partition(pt=20141020) select * from autolog where pt=20141020
    -- insert overwrite local directory '/tmp/autolog' select * from autolog;
    
-- select
    -- select `(ds|hr)?+.+` from autolog limit 10;
    
-- streaming
    -- 使用 python编写的udf/udaf做数据的处理，需要配合sort distribute cluster等操作    
