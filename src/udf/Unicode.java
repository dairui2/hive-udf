package udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Unicode extends UDF{
    public static void main(String[] args) {
//    	Unicode uic = new Unicode();
//    	System.out.println(uic.evaluate(new Text("\\\\u4fdd\\\\u9669\\\\u90e8\\\\u6743\\\\u9650")));
//    	String cn = "保险理财权限";
//        System.out.println(cnToUnicode(cn));
        
        // 字符串 : \u5f00\u59cb\u4efb\u52a1 ，由于 \ 在java里是转义字符，要写出下面这种形式
        String unicode = "\\u4fdd\\u9669\\u90e8\\u6743\\u9650";
        System.out.println(unicodeToCn(unicode));
    }
    private static String unicodeToCn(String unicode) {
        /** 以 \ u 分割，因为java注释也能识别unicode，因此中间加了一个空格*/
        String[] strs = unicode.split("\\\\u");
        String returnStr = "";
        // 由于unicode字符串以 \ u 开头，因此分割出的第一个字符是""。
        for (int i = 1; i < strs.length; i++) {
          returnStr += (char) Integer.valueOf(strs[i], 16).intValue();
        }
        return returnStr;
    }
     
//    private static String cnToUnicode(String cn) {
//        char[] chars = cn.toCharArray();
//        String returnStr = "";
//        for (int i = 0; i < chars.length; i++) {
//          returnStr += "\\u" + Integer.toString(chars[i], 16);
//        }
//        return returnStr;
//    }
}