import javax.xml.soap.Text;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCounter{

	public static void main(String[] args) throws Exception {
		String input = null;
		String output = null;
		try {
			input = args[0];
			output = args[1];
            System.out.println("INPUT : " + input);
            System.out.println("OUTPUT: " + output);
		}
		catch (Exception ex){
			System.out.println("No input or output");
		}

		Job job = new Job();
		job.setJobName("Word Counter");
		job.setJarByClass(WordCounter.class);
		job.setMapperClass(WCMapper.class); 
		job.setReducerClass(WCReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1); 
	}
}
