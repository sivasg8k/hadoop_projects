package com.hadoop.white.ch7.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.white.ch7.examples.SmallFilesToSeqFileConverterJob.SequenceFileMapper;

public class SeqFileToTextConverterJob extends Configured implements Tool {
	
	public static class TextMapper extends Mapper<Text,BytesWritable,NullWritable,Text> {
		
		@Override
		protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			context.write(NullWritable.get(), new Text(value.getBytes()));
		}
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		int result = ToolRunner.run(new SeqFileToTextConverterJob(), args);
        System.exit(result);

	}

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		Job job = new Job(conf);
		
		job.setJarByClass(SeqFileToTextConverterJob.class);
		job.setJobName("seq2text");
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(FileOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(TextMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		
		SequenceFileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
		
	}

}
