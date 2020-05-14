package SecondarySort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SecondSortDriver{
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
		System.err.println("Usage: secondsort <in> <out>");
		System.exit(2);
		}
		Job job = Job.getInstance(conf, "second sort");
		job.setJarByClass(SecondSortDriver.class);
		job.setMapperClass(SecondSortMapper.class);
		job.setReducerClass(SecondSortReducer.class);

		// 通过pair中的第一个值分组
		job.setPartitionerClass(L1Partitioner.class);
		job.setGroupingComparatorClass(SecondSortGroupingComparator.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		// map输出类型为 Pair_Int, IntWritable，value实际为空(NullWritable)
		job.setMapOutputKeyClass(Pair_Int.class);
		job.setMapOutputValueClass(NullWritable.class);

		// reduce输出类型为 Pair_Int, IntWritable，value实际为空(NullWritable)，key调用intpair的tostring，输出text流
		job.setOutputKeyClass(Pair_Int.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

	}
