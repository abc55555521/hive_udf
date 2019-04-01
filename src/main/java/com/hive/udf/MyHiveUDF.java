/**
 * 
 */
package com.hive.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * @author lyl
 * hive > add jar /opt/datas/my_udf.jar 
 * create temporary function my_udf as 'com.hive.udf.MyHiveUDF';	#创建hive的函数
 * show functions; #检查函数是否创建成功
 * 测试：select ename,my_udf(ename) as lower_ename from emp;
 */
public class MyHiveUDF extends UDF{
	
	public Text evaluate(Text content) {
		return evaluate(content, new IntWritable(0));
	}
	
	 
	public Text evaluate(Text content, IntWritable flag) {
		if (content==null)
			return null;
		
		if(flag.get()==1) {
			return new Text(content.toString().toUpperCase());
		}else {
			return new Text(content.toString().toLowerCase()); 
		}		
	}

}
