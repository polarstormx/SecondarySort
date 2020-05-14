package SecondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Pair_Int implements WritableComparable<Pair_Int> {

	private int lf = 0;
	private int rt = 0;

	// 必须重载的读入和输出方法，为了利用编码范围，需要加上偏移量
	@Override
	public void readFields(DataInput dI) throws IOException {

		lf = dI.readInt() + Integer.MIN_VALUE;
		rt = Integer.MAX_VALUE - dI.readInt();
	}

	@Override
	public void write(DataOutput dO) throws IOException {

		dO.writeInt(lf - Integer.MIN_VALUE);
		dO.writeInt(Integer.MAX_VALUE - rt);
	}

	@Override
	public int compareTo(Pair_Int pI) {
		// 第一列的值升序排列，而第二列的值降序排列

		if (lf != pI.lf) {
			return lf < pI.lf ? -1 : 1;
		} else if (rt != pI.rt) {
			return rt > pI.rt ? -1 : 1;
		} else {
			return 0;
		}
	}

	@Override
	public String toString() {
		return this.lf + "\t" + this.rt;
	}

	@Override
	public int hashCode() {
		// 由于第二列的值的范围不超过100，因此使用此函数即可确保任意不同的一二列值对的哈希码不同
		return lf * 100 + rt;
	}

	@Override
	public boolean equals(Object obj) {

		// 当且仅当两列完全相同时相等，不同类型，存在不相等均返回false
		if (obj instanceof Pair_Int) {
			Pair_Int p = (Pair_Int) obj;
			return p.lf == lf && p.rt == rt;
		} else {
			return false;
		}
	}

	public static class SSComparator extends WritableComparator {
		public SSComparator() {
			super(Pair_Int.class);
		}

		// 使用二进制字典序排列
		@Override
		public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
	
			return compareBytes(arg0, arg1, arg2, arg3, arg4, arg5);
		}
	}

	// 在WritableCoparator里激活新定义的comparator
	static {
		WritableComparator.define(Pair_Int.class, new SSComparator());
	}

	// 常规的设置或获取函数
	public int getLf() {
		return lf;
	}

	public int getRt() {
		return rt;
	}

	public void set(int l, int r) {
		lf = l;
		rt = r;
	}

}
