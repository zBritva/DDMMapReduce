import javax.xml.soap.Text;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCounter{

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJobName("Word Counter");
		job.setJarByClass(WordCounter.class);
		job.setMapperClass(WCMapper.class); 
		job.setReducerClass(WCReducer.class); 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/user/zbritva/source.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/user/zbritva/output.txt"));
		System.exit(job.waitForCompletion(true) ? 0 : 1); 
	}
}
