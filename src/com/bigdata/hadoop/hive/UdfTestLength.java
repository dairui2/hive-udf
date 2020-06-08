package com.bigdata.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
public class UdfTestLength extends UDF{

    public Integer evaluate(String s)
    {
        if(s==null)
        {
            return null;
        }else{
            return s.length();
        }
    }
    
//    public static void main(String[] args) {
//    	UdfTestLength ut = new UdfTestLength();
//		System.out.println(ut.evaluate(new String("abcdefg")));
//		System.out.println(ut.evaluate(new String("")));
//	}
}