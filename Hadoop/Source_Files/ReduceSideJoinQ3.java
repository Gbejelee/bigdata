package CS6350.cs6350;
import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import CS6350.cs6350.RatingQ2.BusinessMap;
import CS6350.cs6350.RatingQ2.Reduce;


public class ReduceSideJoinQ3 {
	
	public static class Job2Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text outKey = new Text();
		private Text outValue = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		
			String[] ratingDetails = value.toString().trim().split("\t");
			String bIdKey = ratingDetails[0].trim();
			String ratingValue = ratingDetails[1].trim();
			outKey.set(bIdKey);
			outValue.set("RATING" + ratingValue);
			context.write(outKey, outValue);
		
		}
	}
	
public static class Job2Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text outKey = new Text();
		private Text outValue = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
				String delims = "::";
				String[] businessData = StringUtils.split((String)value.toString(), delims);
				String red_key = businessData[0];
				outKey.set( red_key);
				outValue.set("B_DETAILS" + businessData[1].trim() + " " + businessData[2].trim());
				context.write(outKey, outValue);
			}	
		
		}

public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

		private Text businessIdKey = new Text();
		private Text businessDetailsValue = new Text();
	
		protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
			String rating = null;
			String bDetails = null;
			boolean ratingPresent = false, detailsPresent = false;
			
			for (Text text : values) {
				String[] value = StringUtils.split((String)text.toString(),(String)(" "));
				if(value[0].contains("RATING")){
					rating = text.toString().replace("RATING","");
					ratingPresent = true;
				}
				if (value[0].contains("B_DETAILS")){
					bDetails = text.toString().replace("B_DETAILS","");
					detailsPresent = true;
				}
			}
			if(ratingPresent && detailsPresent){
				businessIdKey.set(key);
				businessDetailsValue.set(new Text(bDetails) + " " + new Text(rating));
				context.write(businessIdKey, new Text(businessDetailsValue.toString()));
			}
		}
}
public static class ChainJobs extends Configured implements Tool {

	 private static final String OUTPUT_PATH = "/intermediate_output";

	 public int run(String[] args) throws Exception {
		 
	  /* Job 1 */
		 
	  Job job1, job2;
	  Configuration conf = getConf();
	  job1 = Job.getInstance(conf, "RatingQ2Job");
	  job1.setJarByClass(ChainJobs.class);
	  
	  job1.setMapperClass(BusinessMap.class);
      job1.setReducerClass(Reduce.class);
      job1.setOutputKeyClass(Text.class);
      job1.setMapOutputValueClass(DoubleWritable.class);
      job1.setOutputValueClass(Text.class);

	  job1.setInputFormatClass(TextInputFormat.class);
	  job1.setOutputFormatClass(TextOutputFormat.class);

	  TextInputFormat.addInputPath(job1, new Path(args[0]));
	  TextOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));

	  boolean bJobComp = job1.waitForCompletion(true);
	 // System.out.println("First job completed" );


	  /* Job 2 */
	  
	  if (bJobComp) {
		  Configuration conf2 = getConf();
		  job2 = Job.getInstance(conf2, "Rating_reduce_join");
		  job2.setJarByClass(ChainJobs.class);
		  FileSystem fs = FileSystem.get(conf2);
			if (fs.exists(new Path(args[2]))) {
				fs.delete(new Path(args[2]), true);
			}
		  
		  job2.setReducerClass(JoinReducer.class);
			
		  job2.setOutputKeyClass(Text.class);
		  job2.setOutputValueClass(Text.class);
	
		  job2.setInputFormatClass(TextInputFormat.class);
		  job2.setOutputFormatClass(TextOutputFormat.class);
	
		  MultipleInputs.addInputPath(job2, new Path(OUTPUT_PATH+ "/part-r-00000"),TextInputFormat.class, Job2Mapper1.class);
		  MultipleInputs.addInputPath(job2, new Path(args[1]),TextInputFormat.class, Job2Mapper2.class);
		  	  
		  TextOutputFormat.setOutputPath(job2, new Path(args[2]));
		  return job2.waitForCompletion(true) ? 0 : 1;
		  
	  }
	  return 1;
	 }
	}
	 /**
	  * Method Name: main Return type: none Purpose:Read the arguments from
	  * command line and run the Job till completion
	  * 
	  */
	 public static void main(String[] args) throws Exception {

		 if (args.length != 3) {
		  System.err.println("Enter valid number of arguments <Inputdirectory1> <Inputdirectory2> <Outputlocation>");
		  System.exit(0);
	  }
	  
	  	  ToolRunner.run(new Configuration(), new ChainJobs(), args);
	 }

	
}
