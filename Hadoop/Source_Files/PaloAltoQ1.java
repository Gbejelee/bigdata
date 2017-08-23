package CS6350.cs6350;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.lang.StringUtils;

public class PaloAltoQ1
{
	public static class Map extends Mapper<LongWritable, Text, Text, NullWritable>
	{
		Context context;
		String filterText = "Palo Alto";
		String fileName = "business.csv";
		final String regex = "(?:List\\()(.*)[*^)]";
		public void map(LongWritable key, Text value, Context context) 
				throws IOException,InterruptedException
		{	
			if(fileName.equals("business.csv")){
				String delims = "::";
				String[] businessData = StringUtils.split((String)value.toString(), (String)delims);
				String red_key = businessData[2];
				if(businessData[1].contains(filterText)){
					final Pattern pattern = Pattern.compile(regex);
					final Matcher matcher = pattern.matcher(red_key);
					if (matcher.find()) {
						String[] result = matcher.group(1).split(",");
						for(int i = 0;i < result.length;i++ ){
							context.write(new Text(result[i]), NullWritable.get());		
						}
					}
				} 
		
			}
		}
							 
	}

	public static class Reduce
	extends Reducer<Text,NullWritable,Text,NullWritable> 
	{
		public void reduce(Text key, Iterable<NullWritable> values,Context context) 
				throws IOException, InterruptedException 
		{
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "PaloAltoQ1");
		
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		 if (otherArgs.length != 2) {
	            System.err.println("Usage: Question1 <business.csv> <output>");
	            System.exit(2);
	        }
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
		
		job.setJarByClass(PaloAltoQ1.class);
		job.setJobName("PaloAltoQ1");
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

	
}

