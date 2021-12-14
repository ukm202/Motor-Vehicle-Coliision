import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Observation: if the line ends with consecutive empty cells i.e. commas,
        // the split function ignores them, so the last few fields are missing.
        // Manually adding placeholder to the end of the line fixes this.
        String rawLine = value.toString() + "ENDOFLINE";
        String line;

        long commaCount = rawLine.chars().filter(ch -> ch == ',').count();
        if (commaCount > 28) {
            // The Location column sometimes include comma within a cell
            if (rawLine.matches(".*\\(.*\\).*")) {
                line = rawLine.replaceAll("\\(.*\\)", "");
            } else { // The address(es) sometimes include comma within a cell
                line = rawLine.replaceAll("\"(.*),(.*)\"", "$1$2");
            }
        } else {
            line = rawLine;
        }

        String[] fields = line.split(",");
        if (fields[fields.length-1].contains("ENDOFLINE")) {
            fields[fields.length-1] = fields[fields.length-1].replace("ENDOFLINE", "");
        }

        String[] newEntries = new String[27];
        // Some records are apparently repeated. Adding key to output ensures all records are unique.
        newEntries[0] = key.toString();

        int newIndex = 1;
        if (fields.length == 29) {
            for (int i=0; i<fields.length; i++) {
                if (i != 6 && i != 10 && i != 11) {
                    if (fields[i].isEmpty()) {
                        newEntries[newIndex] = "-1";
                    } else {
                        newEntries[newIndex] = fields[i];
                    }
                    newIndex += 1;
                }
            }
        } else { // If the line is still poorly formatted after previous processing, fill all fields with "-1"
            for (int i=1; i<newEntries.length; i++) {
                newEntries[i] = "-1";
            }
        }
        String output = String.join(",", newEntries);
        context.write(new Text(output), new IntWritable(1));

    }
}