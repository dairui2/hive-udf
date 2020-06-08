package udf;

import static org.junit.Assert.assertArrayEquals;

import javax.print.attribute.standard.MediaName;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import edu.umd.cs.findbugs.annotations.NoWarning;
 
public class RowNumUDF extends UDF{
     
    private static final Text Text = null;
	public static String signature = "_";
    public static int order = 0;
     
    public int evaluate(Text text){
         
        if(text != null){
             
            //分组排序的依据，列名，通常为主键
            String colName = text.toString();
             
            //处理第一条数据
            if(signature == "_"){
                 
                //记下分组排序的字段：主键，并将rownum设为1
                signature = colName;
                order = 1;
                 
                //返回rownum
                return order;
            }else{
            //首先比对是否和上一条主键相同
            if(signature.equals(colName)){
                 
                //rownum依次加1
                order++;
                return order;
            }else{
                    //如果主键改变，将rownum设为1
                    signature = colName;
                    order = 1;
                    return order;
                }
            }
        }else{
            //如果主键为空，则返回-1
            return -1;
        }
    }
    
    public static void main(String[] args) {
    	RowNumUDF rn = new RowNumUDF();
		System.out.println(rn.evaluate(new Text( "zhangsan")));
	}
}
