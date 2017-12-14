package withcombiner;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/***
 * MinMaxWithCombiner is the driver class for the mapreduce program
 */
public class MinMaxWithCombiner {

  /***
   * MinMaxMapper: Mapper class which contains map function
   * which produces stationID,WeatherTempWritable as the key,value
   */
  public static class MinMaxMapper 
       extends Mapper<Object, Text, Text, WeatherTempWritable>{

	  /***
	     * map : The input station record information is split by ',' to fetch individual fields. 
	     * WeatherTempWritable object is created with type either "TMIN" or "TMAX"
	     * depending upon if its a max or min temperature record.
	     * The same is emitted from mapper using context.
	     * @param key : Input key to mapper.
	     * @param value : contains the line from the input file that has the station temperature information.
	     * @param context : Used to emit output from Mapper
	     * @throws IOException
	     * @throws InterruptedException
	     */
	  public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		  
    	String entry = value.toString();
    	String[] values = entry.split(",");
    
    	WeatherTempWritable record;
    	if(values[2].trim().equals("TMAX")) {
    		if(values[3].trim() != "" || values[3].trim() != null) {
    			record = new WeatherTempWritable(Long.parseLong(values[3].trim()), 1, 0, 0);
        		context.write(new Text(values[0].trim()), record);
    		}
    	}  else if(values[2].trim().equals("TMIN")) {
    		if(values[3].trim() != "" || values[3].trim() != null) {
    			record = new WeatherTempWritable(0, 0, Long.parseLong(values[3].trim()), 1);
        		context.write(new Text(values[0].trim()), record);	
           	}
    	}
    }
  }
  
  /***
   * TemperatureCombiner : Combines multiple temperature records for same stationID 
   * into a single record with the sums and counts accumulated.
   */
  public static class TemperatureCombiner 
  	extends Reducer<Text,WeatherTempWritable, Text, WeatherTempWritable> {
	  
	/**
	 * combines the list of WeatherTempWriyable for the given stationID into a single 
	 * WeatherTtempWritable object with accumulated sums and counts for both max and min temperatures.
	 * @param key - stationID
	 * @param values - list of objects of WeatherTempWritable type for the key stationID
	 * @param context : used to emit records from combiner to temp files
	 * @throws IOException
	 * @throws InterruptedException
	 */  
	public void reduce(Text key, Iterable<WeatherTempWritable> values, Context context)
			throws IOException, InterruptedException { 
		long maxSum = 0;
		long minSum = 0;
		int minCount = 0;
		int maxCount = 0;
		
		/** Iterate over the temperature records for the given stationID key.
		  * and aggregate the sum and count of temperatures for each both TMIN and TMAX types.
		  * Finally find the mean temperatures for both min and max if the count is higher than 0.
		  */
		for (WeatherTempWritable val : values) {
			maxSum = maxSum + val.getMaxSum();
			maxCount = maxCount+ val.getMaxCount();				 
		    minSum = minSum + val.getMinSum();
		   	minCount = minCount + val.getMinCount();
		}
		   
		WeatherTempWritable record;
		 record = new WeatherTempWritable(maxSum, maxCount, minSum, minCount); 
		 		 
		 context.write(key,  record);
	 }
  }
  
  /***
   * MinMaxReducer : Reduce task is created per stationID
   */
  public static class MinMaxReducer 
       extends Reducer<Text,WeatherTempWritable,NullWritable,Text> {

	  /***
	     * reduce: The mean max and mean min temperatures for the incoming key(stationID) is aggregated in loop 
	     * and the corresponding average is generated as output from the reducer.
	     * @param key : stationID
	     * @param values: Contains the list of WeatherTempWritable records which contains the temperature records.
	     * @param context: Used to emit output from reducer
	     * @throws IOException
	     * @throws InterruptedException
	     */
    public void reduce(Text key, Iterable<WeatherTempWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    	int maxCount = 0;
    	int minCount = 0;
    	double maxSum = 0;
    	double minSum = 0;
        String result= key+",";
        
      /* Iterate over the temperature records for the given stationID key.
	   * and aggregate the sum and count of temperatures for each both TMIN and TMAX types.
	   * Finally find the mean temperatures for both min and max if the count is higher than 0.
	   */
      for (WeatherTempWritable val : values) {
      		  maxSum = maxSum + val.getMaxSum();
    		  maxCount = maxCount + val.getMaxCount();
       		  minSum = minSum + val.getMinSum();
       		  minCount = minCount + val.getMinCount();
      }
          
      double meanMinTemp=0;
      double meanMaxTemp=0;
    
      try {
    	  meanMinTemp = minSum/minCount;
    	  meanMaxTemp = maxSum/maxCount;
      }catch(ArithmeticException e) {
    	  
     }
 
      result=result+meanMinTemp+",";
      result=result+meanMaxTemp;
      //System.out.println(result);
      
      context.write(NullWritable.get(), new Text(result));
    }
  }

  /***
   * Main : Setups up the mapreduce environment configuration.
   * This program involves only Mapper and Reducer. The Map output key is stationID, 
   * output value is of type WeatherTempWritable
   * Input file name and output directory configuration is read from args
   * @param args : contains two parameters input file name and the output directory name.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: withcombiner <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "mean temperatures with combiner");
    job.setJarByClass(MinMaxWithCombiner.class);
    job.setMapperClass(MinMaxMapper.class);
    job.setCombinerClass(TemperatureCombiner.class);
    job.setReducerClass(MinMaxReducer.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(WeatherTempWritable.class);
    
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

