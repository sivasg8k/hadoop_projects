package com.hadoop.white.ch7.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class PDFWordCount extends Configured implements Tool {

	public static class PDFWordCountMap extends Mapper<NullWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
       
        public void map (NullWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            
            StringTokenizer tokenizer = new StringTokenizer(line,"\n");
            while(tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
	}
   
    public static class PDFWordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value: values) {
                    sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = getConf();
		
		Job job = new Job(conf);
		job.setJarByClass(PDFWordCount.class);
		job.setJobName("pdfwordcount");
		
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setMapperClass(PDFWordCountMap.class);
	    job.setReducerClass(PDFWordCountReducer.class);
	    
	   
	    job.setInputFormatClass(PDFFileFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	   
	    PDFFileFormat.setInputPaths(job, new Path(args[0]));
	    TextOutputFormat.setOutputPath(job, new Path(args[1]));
	   
	    boolean success = job.waitForCompletion(true);
	    return success ? 0: 1;
    }
	   
	    /**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
	    // TODO Auto-generated method stub
        int result = ToolRunner.run(new PDFWordCount(), args);
        System.exit(result);
    }

}
