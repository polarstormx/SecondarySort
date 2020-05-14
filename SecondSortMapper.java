package SecondarySort;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondSortMapper
         extends Mapper<Text, Text, Pair_Int, NullWritable> {

    private final Pair_Int key = new Pair_Int();

    @Override
    public void map(Text key, Text value,Context context) throws IOException, InterruptedException {
      this.key.set(Integer.parseInt(key.toString()), Integer.parseInt(value.toString()));
      context.write(this.key, NullWritable.get());//key中为intpair，value为null
    }
 }