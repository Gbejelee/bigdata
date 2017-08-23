package CS6350.cs6350;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class InMemoryJoinQ4 {
	public  static class Map extends Mapper<LongWritable, Text, Text, Text> {

		HashSet<String> businessIdSet = new HashSet<String>();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) 
				throws IOException, InterruptedException {	
		
			Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			//URI[] cacheFilesLocal = context.getCacheFiles();
			if(cacheFilesLocal != null && cacheFilesLocal.length > 0) {
				    for(Path cacheFile : cacheFilesLocal) {
				    		readFile(cacheFile);
				    		System.out.println("inside readFile");
				    }
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			System.out.println("Number of elements with stanford : "+businessIdSet.size());
			
			String[] reviewDetails = value.toString().split("::");
			if (reviewDetails.length == 4 && businessIdSet.contains(reviewDetails[2])) {
				context.write(new Text(reviewDetails[1]), new Text(reviewDetails[3]));
			}
		}
		
		 private void readFile(Path filePath) {
			 try{
				BufferedReader br = null;
				String line = null;
				br = new BufferedReader(new FileReader(filePath.toString()));
				line = br.readLine();
				int count = 0;
				while (line != null) {
					String[] businessDetails = line.split("::");
						if (businessDetails.length > 2 && businessDetails[1].contains("Stanford"))
						{
							businessIdSet.add(businessDetails[0]);
							count++;
						}
						line = br.readLine();
					}
				System.out.println("Count : "+ count);
		    }catch(IOException ex) {
				System.err.println("Exception while reading stop words file: " + ex.getMessage());
		    }
		 }
	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: InMemoryJoinQ4 <review.csv dir> <business.csv dir> <output dir>");
			System.exit(2);
		}

		conf.set("businessFile", otherArgs[1]);

		Job job = Job.getInstance(conf, "InMemoryJoinQ4");
		job.setJarByClass(InMemoryJoinQ4.class);

		job.setMapperClass(Map.class);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[2]))) {
			fs.delete(new Path(args[2]), true);
		}

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.addCacheFile(new Path(args[1]).toUri());

		//DistributedCache.addCacheFile(new Path(args[1]).toUri(), job.getConfiguration());

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
