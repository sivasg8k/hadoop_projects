package com.hadoop.white.ch7.examples;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class PDFFileFormat extends FileInputFormat<NullWritable, Text> {

	@Override
	public RecordReader<NullWritable, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		
		PDFRecordReader pdfRecordReader = new PDFRecordReader();
		
		pdfRecordReader.initialize(split, context);
		
		return pdfRecordReader;
	}
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
	    return false;
	}

}
