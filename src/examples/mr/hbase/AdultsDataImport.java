package examples.mr.hbase;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AdultsDataImport extends Configured implements Tool {

	public static class AdultDataMap extends Mapper<LongWritable, Text, Text, Text> {
        
        private static int num = 0;
        
        private Text rowKey = new Text();
       
        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] elems = line.split(",");
            num++;
            String rowKeyStr = null;
            
            if(elems.length > 3) {
            	rowKeyStr = elems[0].trim() + "-" + elems[3].trim() + "-" + num;
            } else {
            	rowKeyStr = "" + num;
            	
            }
            rowKey.set(rowKeyStr);
            context.write(rowKey,value);
           
        }
	}
   
    public int run(String[] args) throws Exception {
		
    	Job job = new Job(getConf());
		job.setJarByClass(AdultsDataImport.class);
		job.setJobName("adultImport");
	   
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	   
	    job.setMapperClass(AdultDataMap.class);
	    job.setNumReduceTasks(0);
	   
	   
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	   
	   
	    TextInputFormat.setInputPaths(job, new Path(args[0]));
	    SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
	   
	    boolean success = job.waitForCompletion(true);
	    return success ? 0: 1;
    }
	   
	    /**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
	    // TODO Auto-generated method stub
        int result = ToolRunner.run(new AdultsDataImport(), args);
        System.exit(result);
    }

}
