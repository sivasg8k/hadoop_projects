package com.hadoop.white.ch7.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.parser.PdfTextExtractor;

public class PDFRecordReader extends RecordReader<NullWritable, Text> {
	
	private FileSplit fileSplit = null;
	private Configuration conf = null;
	private boolean processed = false;
	private Text value = new Text();

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		this.fileSplit = (FileSplit)split;
		this.conf = context.getConfiguration();
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			
			StringBuffer sb = new StringBuffer();
			
			Path file = fileSplit.getPath();
			
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream in = null;
			PdfReader reader = null;
			try {
				
				in = fs.open(file);
				reader = new PdfReader(in);
				int numPages = reader.getNumberOfPages();
				
				for(int i=1;i<=numPages;i++) {
		        	String page = PdfTextExtractor.getTextFromPage(reader, i);
		        	sb.append(page);
		        }
			
			} catch(Exception e) {
				
			}
			value.set(sb.toString());
			processed = true;
			return true;
		}
		return false;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException,
			InterruptedException {
		return NullWritable.get();
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
}
