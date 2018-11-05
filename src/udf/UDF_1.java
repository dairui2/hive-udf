package udf;

import org.apache.hadoop.hive.ql.exec.UDF;

// 添加jar包：hive>add jar hdfs://xp-160:8020/user/hadoop/hongxingfan/jars/udf.jar;
// 创建临时函数：hive>create temporary function udf as 'udf.UDF_1';
// 使用UDF：hive>select udf(name) from student;  
public class UDF_1 extends UDF {
	public String evaluate() {
		return "hello world!";
	}

	public String evaluate(String str) {
		return "hello world: " + str;
	}
}
