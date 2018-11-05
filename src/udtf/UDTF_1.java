package udtf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @描述：udtf是为行转列时使用的
 * @时间：2014年10月14日 下午12:17:54
 * @作者：hongxingfan
 * 
 * @评价：效率比较低
 * @学到：UDTF有两种使用方法，一种直接放到select后面，一种和lateral view一起使用。
 */
// 1：直接select中使用
// select udtf_1(properties) as (col1,col2) from src;

// 不可以添加其他字段使用
// select a, udtf_1(properties) as (col1,col2) from src

// 不可以嵌套调用
// select udtf_1(explode_map(properties)) from src

// 不可以和group by/cluster by/distribute by/sort by一起使用
// select udtf_1(properties) as (col1,col2) from src group by col1, col2

// 2：和lateral view一起使用
// select src.id, mytable.col1, mytable.col2 from src lateral view udtf_1(properties) mytable as col1, col2;
// 此方法更为方便日常使用。执行过程相当于单独执行了两次抽取，然后union到一个表里。
public class UDTF_1 extends GenericUDTF {

	@Override
	public void close() throws HiveException {
	}

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		if (args.length != 1) {
			throw new UDFArgumentLengthException("ExplodeMap takes only one argument");
		}
		if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentException("ExplodeMap takes string as a parameter");
		}

		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		fieldNames.add("col1");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("col2");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	@Override
	public void process(Object[] args) throws HiveException {
		String input = args[0].toString();
		String[] test = input.split(";"); // 行转列
		for (int i = 0; i < test.length; i++) {
			try {
				String[] result = test[i].split(":");
				forward(result);
			} catch (Exception e) {
				continue;
			}
		}
	}
}