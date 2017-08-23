package CS6350.cs6350;
import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Comparator;

public class RatingQ2 {
	
	public static class BusinessMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String delims = "::";
	        String[] reviewData = StringUtils.split((String)value.toString(), delims);
	        if (reviewData.length == 4) {
	            try {
	                double rating = Double.parseDouble(reviewData[3]);
	                context.write(new Text(reviewData[2]), new DoubleWritable(rating));
	            }
	            catch (NumberFormatException e) {
	                context.write(new Text(reviewData[2]), new DoubleWritable(0.0));
	            }
	        }
	    }
	}

	public static class AvgCompare implements Comparator<Map.Entry<String, Double>> {
		AvgCompare() {
	    }
	    public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
	        return o2.getValue().compareTo(o1.getValue());
	    }
	}
	    
	public static class Reduce extends Reducer<Text, DoubleWritable, Text, Text> {
	    private Map<String, Double> countMap = new HashMap<String, Double>();
	  
	    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
	        int count = 0;
	        double sum = 0.0;
	        for (DoubleWritable val : values) {
	            sum += val.get();
	            ++count;
	        }
	        Double avg = sum / (double)count;
	        this.countMap.put(key.toString(), avg);
	    }
	    @Override
	    protected void cleanup(Reducer<Text, DoubleWritable, Text, Text>.Context context) throws IOException, InterruptedException {
	        Set<Map.Entry<String, Double>> set = this.countMap.entrySet();
	        ArrayList<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(set);
	        Collections.sort(list,new AvgCompare());
	        int counter = 0;
	        for (Map.Entry<String, Double> entry : list) {
	            if (counter < 10){
	            	context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
	            	++counter;
	            }
	        }
	    }
	}
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Question2 <review.csv> <output>");
            System.exit(2);
        }
		
        Job job = Job.getInstance(conf, "RatingQ2");
        job.setJarByClass(RatingQ2.class);
        job.setMapperClass(BusinessMap.class);
        
        FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(otherArgs[1]))) {
			fs.delete(new Path(otherArgs[1]), true);
		}
        
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath((Job)job, (Path)new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath((Job)job, (Path)new Path(otherArgs[1]));
        job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

