package examples.mr.bookcount;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BookCounter extends Configured implements Tool {

	public static class BookCounterMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
       
        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
        	String line = value.toString();
        	line = line.replace("\"", "");
        	String[] inp = line.split(";");
        	inp[3] = inp[3].trim();
        	word.set(inp[3]);
        	context.write(word, one);
            
        }
	}
   
    public static class BookCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            
        	int sum=0;
        	for(IntWritable value: values) {
            	sum = sum + value.get();
            }
            context.write(key, new IntWritable(sum));
            
        }
    }
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job(getConf());
		job.setJarByClass(BookCounter.class);
		job.setJobName("Books frequency");
	   
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	   
	    job.setMapperClass(BookCounterMap.class);
	  //  job.setCombinerClass(AnagramCounterReducer.class);
	    job.setReducerClass(BookCounterReducer.class);
	   
	   
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	   
	   
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	   
	    boolean success = job.waitForCompletion(true);
	    return success ? 0: 1;
    }
	   
	    /**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
	    // TODO Auto-generated method stub
        int result = ToolRunner.run(new BookCounter(), args);
        System.exit(result);
    }

}
