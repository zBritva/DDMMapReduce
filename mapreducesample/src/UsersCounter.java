/**
 * Created by zbritva on 02.03.16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;

public class UsersCounter {

    public static class LogMap extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
        private String ip, user = new String();
        private Text one = new Text("1");

        public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] data = line.split("\t");

            this.ip = data[0].trim();
            this.user = data[1].trim();
            output.collect(new Text(this.ip), this.one);
        }
    }

    public static class CountryMap extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
        private String ip, country = new String();

        public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] data = line.split("\t");

            this.ip = data[0].trim();
            this.country = data[1].trim();
            output.collect(new Text(this.ip), new Text(this.country));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        String merge = "";
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            int i = 0;
            while (values.hasNext()) {
                if (i == 0) {
                    merge = values.next().toString() + ",";
                } else {
                    merge += values.next().toString();
                }
                i++;
            }
            output.collect(key, new Text(merge));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration c=new Configuration();
        String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
        Path p1=new Path(files[0]);
        Path p2=new Path(files[1]);
        Path p3=new Path(files[2]);
        FileSystem fs = FileSystem.get(c);
        if(fs.exists(p3)){
            fs.delete(p3, true);
        }

        JobConf conf = new JobConf(c, UsersCounter.class);
        conf.setJobName("user counter");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);


        MultipleInputs.addInputPath(conf, p1, TextInputFormat.class, CountryMap.class);
        MultipleInputs.addInputPath(conf,p2, TextInputFormat.class, LogMap.class);
        //conf.setMapperClass(Map.class);
        //conf.setCombinerClass(WCS.Reduce.class);
        conf.setReducerClass(Reduce.class);

        //conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        //FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, p3);


        JobClient.runJob(conf);
    }
}
