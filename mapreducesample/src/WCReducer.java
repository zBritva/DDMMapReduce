import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Mapper.Context context) throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable value : values) {
			count += value.get(); 
		}
		context.write(key, new IntWritable(count)); 
	}
}
