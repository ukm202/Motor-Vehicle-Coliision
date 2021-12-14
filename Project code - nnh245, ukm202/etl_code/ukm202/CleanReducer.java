
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
public class CleanReducer
        extends Reducer<Text, IntWritable, Text, NullWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
      int counter = 0;
      for (IntWritable value : values) {
            counter += value.get();
      }
        
      context.write(key, NullWritable.get());
    }
}
