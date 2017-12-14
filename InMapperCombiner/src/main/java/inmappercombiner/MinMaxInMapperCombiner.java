package inmappercombiner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

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
 * MinMaxInMapperCombiner is the driver class for the mapreduce program
 */
public class MinMaxInMapperCombiner {

  /***
   * MinMaxMapper: Mapper class which contains map function
   * which produces stationID,WeatherTempWritable as the key,value
   */
  public static class MinMaxMapper 
       extends Mapper<Object, Text, Text, WeatherTempWritable>{

	  Map<String, WeatherTempWritable> stationMap;
	  
	  /**
	   * setup: initializes the stationMap data structure
	   * @param context: used to emit the records
	   * @throws IOException
	   * @throws InterruptedException
	   * 
	   */
	  protected void setup(Context context) throws IOException, InterruptedException {
		  stationMap = new HashMap<String, WeatherTempWritable>();
	  }
	    

	  /***
	     * map : The input station record information is split by ',' to fetch individual fields. 
	     * WeatherTempWritable object is created with type either "TMIN" or "TMAX"
	     * depending upon if its a max or min temperature record.
	     * The same is emitted from mapper using context.
	     * @param key : Input key to mapper.
	     * @param value : contains the line from the input file that has the station temperature information
	     * for a given year.
	     * @param context : Used to emit output from Mapper
	     * @throws IOException
	     * @throws InterruptedException
	     */
	  public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		  
    	String entry = value.toString();
    	String[] values = entry.split(",");
    	String stationID = values[0].trim();
        	
    	/**
    	 * Checks whether the stationMap already has the given stationID as the key.
    	 * If its present, fetches the value against it from the hashmap, and adds
    	 * the current temperature value depending on whether its a TMIN or TMAX record.
    	 * Also increments the corresponding count by 1.
    	 * If the stationID is not present in the map, adds a new entry with key-stationID
    	 * and value - a WeatherTempWritable object.
    	 */
    	if(values[2].trim().equals("TMAX")) {
    		if(values[3].trim() != "" || values[3].trim() != null) {
    			if(stationMap.containsKey(stationID)) {
    				WeatherTempWritable record = stationMap.get(stationID);
    				long maxSum = record.getMaxSum();
    				maxSum = maxSum + Long.parseLong(values[3].trim());
    				
    				int maxCount = record.getMaxCount();
    				maxCount = maxCount + 1;
    				
    				stationMap.put(stationID, 
    						new WeatherTempWritable(maxSum,
    								maxCount, record.getMinSum(), record.getMinCount()));
    			} else {
    				stationMap.put(stationID, new WeatherTempWritable(Long.parseLong(values[3].trim()),1, 0, 0));
    			}
       		}
    	}  else if(values[2].trim().equals("TMIN")) {
    		if(values[3].trim() != "" || values[3].trim() != null) {
    			if(stationMap.containsKey(stationID)) {
    				WeatherTempWritable record = stationMap.get(stationID);
    				long minSum = record.getMinSum();
    				minSum = minSum + Long.parseLong(values[3].trim());
    				
    				int count = record.getMinCount();
    				count = count + 1;
    				stationMap.put(stationID, new WeatherTempWritable(
    						record.getMaxSum(), record.getMaxCount(), minSum, count));
    				
    			} else {
    			}
          	}
    	}
    }
	
	/**
     * cleanup : emits the accumulated station information in the stationMap at the end of map call.
     * @param context : to emit records from mapper.
     * @throws IOException
     * @throws InterruptedException
     */
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Iterator<Map.Entry<String, WeatherTempWritable>> it = stationMap.entrySet().iterator();
		  		  
		while(it.hasNext()) {
		  Entry<String, WeatherTempWritable> entry = it.next();
		  context.write(new Text(entry.getKey()), entry.getValue());
		}	  
	  }
  }
  
  /**
   * MinMaxReducer : Reduce task is created per stationID
   */  
  public static class MinMaxReducer 
       extends Reducer<Text,WeatherTempWritable,NullWritable,Text> {

	/**
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
     // System.out.println(result);
      
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
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(MinMaxInMapperCombiner.class);
    job.setMapperClass(MinMaxMapper.class);
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

