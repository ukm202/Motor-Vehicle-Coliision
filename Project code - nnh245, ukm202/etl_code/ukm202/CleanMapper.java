import java.awt.List;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class CleanMapper
        extends Mapper<LongWritable, Text, Text, IntWritable>{
        
        private static final IntWritable  one = new IntWritable(1);
             
        
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
                String data = value.toString();
                String[] field = data.split(",");


                String record = field[0]+","+field[1]+","+field[2]+","+field[3]+","+field[4]+","+field[5]+","+field[6]+","+field[8]+","+field[9];

                

                // if (key.get() == 0 && value.toString().contains("location")) {
                //         return;
                // }
                // else if(field[5].isEmpty()){
                //         return;
                // }
                // else if(Integer.parseInt(field[2]) == 0){
                //         return;
                // }
                // else{
                        
                context.write(new Text(record), one);
                        
                        
                // }
        }
}