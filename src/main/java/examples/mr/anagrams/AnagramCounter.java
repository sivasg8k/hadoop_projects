package examples.mr.anagrams;

import java.io.IOException;
import java.util.Arrays;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class AnagramCounter extends Configured implements Tool {

	public static class AnagramCounterMap extends Mapper<LongWritable, Text, Text, Text> {
        
      //  private Text sortedText = new Text();
       
        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            char[] lineChars = value.toString().toCharArray();
            Arrays.sort(lineChars);
            String sortedWord = new String(lineChars);
            
            Text sortedText = new Text();
            sortedText.set(sortedWord);
            
            context.write(sortedText, value);
            
            System.out.println("Each line current value: "+context.getCurrentValue()+"\t CurrentKey: "+context.getCurrentKey()+"\n=====================\n");
        }
	}
   
    public static class AnagramCounterReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String anagramStr = "";
            System.out.println("key: "+key+"\n CurrentKey: "+context.getCurrentKey());
            for(Text value: values) {
            	anagramStr += value.toString() + "~";
            }
            context.write(key, new Text(anagramStr));
            
        }
    }
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job(getConf());
		job.setJarByClass(AnagramCounter.class);
		job.setJobName("anagrams");
	   
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	   
	    job.setMapperClass(AnagramCounterMap.class);
	  //  job.setCombinerClass(AnagramCounterReducer.class);
	    job.setReducerClass(AnagramCounterReducer.class);
	   
	   
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
        int result = ToolRunner.run(new AnagramCounter(), args);
        System.exit(result);
    }

}
