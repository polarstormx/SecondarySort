package SecondarySort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class L1Partitioner extends Partitioner<Pair_Int, IntWritable> {
	@Override
	// use the first line when parting the pair
	// 保证相同的第一列值必然分配到同一个reducer上
	public int getPartition(Pair_Int key, IntWritable val, int num) {

		return Math.abs(key.getLf() * 100) % num;
	}
}
