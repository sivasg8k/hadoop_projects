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
	private int numPages = 0;
	private int curPage = 1;
	private PdfReader reader =null;
	

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		this.fileSplit = (FileSplit)split;
		this.conf = context.getConfiguration();
		Path file = fileSplit.getPath();
		
		FileSystem fs = file.getFileSystem(conf);
		
		FSDataInputStream in = fs.open(file);
		this.reader = new PdfReader(in);
		numPages = reader.getNumberOfPages();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			
			String page = null;
			
			try {
				  
				   page = PdfTextExtractor.getTextFromPage(reader, curPage);
		    
			} catch(Exception e) {
				
			}
			value.set(page);
			if(curPage == numPages) {
				processed = true;
			} else {
				curPage++;
			}
			
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
