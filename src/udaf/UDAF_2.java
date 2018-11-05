package udaf;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.NumericUDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class UDAF_2 extends NumericUDAF {

	private static class Count {
		public int num;
		public Set<String> set;
	}

	public static class Evaluator implements UDAFEvaluator {
		private Count count;

		public Evaluator() {
			count = new Count();
			init();
		}

		// 相当于mapreduce的setup函数
		@Override
		public void init() {
			count.num = 0;
			count.set = new HashSet<String>();
		}

		// 相当于mapreduce的map函数；可以多参数
		public boolean iterate(String pro) {
			if (pro != null) {
				count.num += 1;
				count.set.add(pro);
			}
			return true;
		}

		// 相当于mapreduce的combiner
		public Count terminatePartial() {
			return count.num == 0 ? null : count;
		}

		// 相当于reducer端的合并
		public boolean merge(Count c) {
			if (c != null) {
				count.num += c.num;
				for (String str : c.set)
					count.set.add(str);
			}
			return true;
		}

		// reduce计算
		public String terminate() {
			return count.num + "\t" + count.set.size();
		}
	}
}
