package com.hadoop.white.ch7.examples;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class NonSplittableTextFileFormat extends TextInputFormat {
		
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

}
