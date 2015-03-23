package com.hadoop.white.ch7.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SmallFilesToSeqFileConverterJob extends Configured implements Tool {
	
	public static class SequenceFileMapper extends Mapper<NullWritable,BytesWritable,Text,BytesWritable> {
		
		private Text fileName;
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();
			fileName = new Text(path.toString());
		}
		
		@Override
		protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
			context.write(fileName, value);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		Job job = new Job(conf);
		
		job.setJarByClass(SmallFilesToSeqFileConverterJob.class);
		job.setJobName("smallfiles");
		
		job.setInputFormatClass(WholeFileFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setMapperClass(SequenceFileMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setReducerClass(Reducer.class);
		//job.setNumReduceTasks(0);
		
		WholeFileFormat.setInputPaths(job, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		
		int result = ToolRunner.run(new SmallFilesToSeqFileConverterJob(), args);
        System.exit(result);
	}
	
	

}
