package udaf;

import org.apache.hadoop.hive.ql.exec.NumericUDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

public class UDAF_1 extends NumericUDAF {

	public static class Count {
		public int num;
		public int maxAge; // 这里怎么不能使用HashSet呢？？？
	}

	public static class Evaluator implements UDAFEvaluator {
		private Count count;

		public Evaluator() {
			System.out.println("Evaluator.......");
			count = new Count();
		}

		// 相当于mapreduce的setup函数；在mapper中并没有走init函数；在reducer中
		public void init() {
			System.out.println("init.......");
			count = new Count();
			count.num = 0;
			count.maxAge = 0;
		}

		// 相当于mapreduce的map函数；可以多参数
		public boolean iterate(IntWritable age) {
			if (age != null) {
				System.out.println("iterate-->" + age);
				count.num += 1;
				count.maxAge = count.maxAge > age.get() ? count.maxAge : age.get();
			}
			return true;
		}

		// 相当于mapreduce的combiner；无参数返回iterate轮训后的值
		public Count terminatePartial() {
			System.out.println("terminatePartial-->");
			return count.num == 0 ? null : count;
		}

		// 相当于reducer端的合并；接收terminatePartial的返回结果，进行数据merge操作
		public boolean merge(Count c) {
			if (c != null) {
				System.out.println("merge-->" + c.maxAge);
				count.num += c.num;
				count.maxAge = count.maxAge > c.maxAge ? count.maxAge : c.maxAge;
			}
			return true;
		}

		// reduce计算
		public String terminate() {
			System.out.println("terminate-->");
			return count.num + "\t" + count.maxAge;
		}
	}
}
