package SecondarySort;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

public class SecondSortGroupingComparator implements RawComparator<SecondarySort.Pair_Int> {
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return WritableComparator.compareBytes(b1, s1, Integer.BYTES, b2, s2, Integer.BYTES);
	}

	@Override
	public int compare(SecondarySort.Pair_Int p1, SecondarySort.Pair_Int p2) {
		return Integer.compare(p1.getLf(), p2.getLf());
	}
}
