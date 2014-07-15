package examples.mr.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
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

public class WordCount extends Configured implements Tool {

	public static class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
       
        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            System.out.println("line: "+line);
            StringTokenizer tokenizer = new StringTokenizer(line);
            while(tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
            System.out.println("Each line current value: "+context.getCurrentValue()+"\t CurrentKey: "+context.getCurrentKey()+"\n=====================\n");
        }
	}
   
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            System.out.println("key: "+key+"\n CurrentKey: "+context.getCurrentKey());
            for(IntWritable value: values) {
                    sum += value.get();
            }
            context.write(key, new IntWritable(sum));
            System.out.println("sum: "+sum);
        }
    }
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = getConf();
		conf.set("mapred.input.dir.recursive", "true");
		
		Job job = new Job(conf);
		job.setJarByClass(WordCount.class);
		job.setJobName("wordcountJ1");
		
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    //job.setMapperClass(Mapper.class);
	    job.setMapperClass(WordCountMap.class);
	  //  job.setCombinerClass(WordCountReducer.class);
	    job.setReducerClass(WordCountReducer.class);
	    //job.setNumReduceTasks(0);
	   
	   
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
        int result = ToolRunner.run(new WordCount(), args);
        System.exit(result);
    }

}
