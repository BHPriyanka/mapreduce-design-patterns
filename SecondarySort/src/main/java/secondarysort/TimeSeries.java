package secondarysort;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/***
 * TimeSeries is the driver class for the mapreduce program
 */
public class TimeSeries {
	
	/***
	 * SecondarySortMapper: Mapper class which contains map function
	 * which produces StationKey,StationInfo as the key,value
	 */
	public static class SecondarySortMapper 
	extends Mapper<Object, Text, StationKey, StationInfo>{
		

		/***
		  * map : The input filename is obtained to determine the year.
		  * The input station record information is split by ',' to fetch individual fields.
		  * StationKey object is created with the StationID and the year 
		  * StationInfo object is created with type either "TMIN" or "TMAX"
		  * depending upon if its a max or min temperature record.
		  * The same is emitted from mapper using context.
		  * @param key : Input key to mapper.
		  * @param value : contains the line from the input file that has the station temperature information
		  * for a given year.
		  * @param context : Used to emit output from Mapper
		  * @throws IOException
		  * @throws InterruptedException
		  */
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			String year = filename.substring(0,4);
						
			String entry = value.toString();
			String[] values = entry.split(",");
  
			StationInfo record;
			if(values[2].trim().equals("TMAX")) {
				if(values[3].trim() != "" || values[3].trim() != null) {
					record = new StationInfo(Long.parseLong(values[3].trim()), 1, 0, 0, Integer.parseInt(year));
					context.write(new StationKey(new Text(values[0].trim()), new IntWritable(Integer.parseInt(year))), record);
				}
			}  else if(values[2].trim().equals("TMIN")) {
				if(values[3].trim() != "" || values[3].trim() != null) {
					record = new StationInfo(0, 0, Long.parseLong(values[3].trim()), 1, Integer.parseInt(year));
					context.write(new StationKey(new Text(values[0].trim()), new IntWritable(Integer.parseInt(year))), record);
				}
			}
		}
	}
	
	/***
	 * KeyComparator: Key Comparator class which contains compare function
	 * to compare two StationKeys
	 */
	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(StationKey.class, true);
		}
		
		/**
		 * compare: Override the superclass method of WritableComparator class
		 * to compare two StatonKey objects.
		 * @param w1: First StationKey object
		 * @param w2: Second StationKey object 2
		 * returns an integer 0, -1 or 1 depending on whether both keys are same or
		 * one is different from other.
		 */
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
		   	StationKey k1 = (StationKey) w1;
		   	StationKey k2 = (StationKey) w2;
		   	int cmp = k1.compareTo(k2);
		   	return cmp;
		}
	}
		  
	/***
	 * GroupComparator: Group Comparator class which contains compare function
	 * to compare two StationKeys based only on the StationIDs.
	 */
	public static class GroupComparator extends WritableComparator {
	    protected GroupComparator() {
	        super(StationKey.class, true);
	    }   
	    
		
		/**
		 * compare: Override the superclass method of WritableComparator class
		 * to compare two StatonKey objects based only on StationID.
		 * This compare function ignores the year in the keys.
		 * @param w1: First StationKey object
		 * @param w2: Second StationKey object 2
		 * returns an integer 0, -1 or 1 depending on whether both keys are same or
		 * one is different from other.
		 */
	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	        StationKey k1 = (StationKey)w1;
	        StationKey k2 = (StationKey)w2;
	         
	        return k1.getStationID().compareTo(k2.getStationID());
	    }
	}
	
	
	/***
	 * KeyPartitioner: Partitioner class to partition the records so that ones
	 * with the same hash value .
	 * Partitions the records based on a hash function result on stationID
	 * so that it ensures that all the records whose stationID has hashed to the same
	 * value ends up in the same reducer for the reduce task.
	 */
	public static class KeyPartitioner extends Partitioner<StationKey, StationInfo> {
		 
	    @Override
	    public int getPartition(StationKey key, StationInfo val, int numPartitions) {
	        int hash = key.getStationID().hashCode();
	        int partition = hash % numPartitions;
	        return Math.abs(partition);
	    }
	 
	}
	
	/**
     * SecondarySortReducer : Reduce task is created per stationID
	 */ 
	public static class SecondarySortReducer 
    extends Reducer<StationKey,StationInfo,NullWritable,Text> {
		
		/**
		   * reduce: The mean max and mean min temperatures for the incoming key(stationKey) is aggregated in loop 
		   * and the corresponding average is generated as output from the reducer.
		   * @param key : stationKey -> Composite key with stationID and year
		   * @param values: Contains the list of StationInfo records which contains the temperature detalils.
		   * @param context: Used to emit output from reducer
		   * @throws IOException
		   * @throws InterruptedException
		   */
		public void reduce(StationKey key, Iterable<StationInfo> values, 
				Context context) throws IOException, InterruptedException {
			
			long maxSum = 0;
			long minSum = 0;
			double meanMinTemp = 0;
			double meanMaxTemp = 0;
			int maxCount = 0;
			int minCount = 0;
			int y = 0;
			
			// resultant string used to display the output in the desired format
			StringBuilder result = new StringBuilder();
			
			result.append(key.getStationID());
			result.append(",");
			result.append("[");
						
			/**
			 * Iterates over each value in the list. 
			 * if the val.year is not 0 or if its not equal to the local y,
			 * computes the mean min and mean max temperature of the accumulated maxSum,
			 * minSum, maxCount and minCount.
			 * At the end, re intializes all the values to 0.
			 * If the val.year matches y, accumulates the maxSum, minSum, maxCount and minCount
			 * and updates the year.
			 */
			for(StationInfo val : values) {
				if(y != 0 && y != val.getYear()) {
					result.append("(");
					result.append(y);
					result.append(",");
					if(minCount > 0) {
						meanMinTemp = minSum/minCount;
						result.append(meanMinTemp);
						
					} else {
						result.append("NULL");
					}
					
					result.append(",");
					
					if(maxCount > 0) {
						meanMaxTemp = maxSum/maxCount;
						result.append(meanMaxTemp);
					} else {
						result.append("NULL");
					}
					result.append(")");
					maxSum=0;
					minSum=0;
					meanMinTemp=0;
					meanMaxTemp=0;
					minCount=0;
					maxCount=0;
					result.append(",");
				} 
							
				maxSum = maxSum + val.getMaxSum();
				minSum = minSum + val.getMinSum();
				maxCount = maxCount + val.getMaxCount();
				minCount = minCount + val.getMinCount();
				y = val.getYear();
			}
						
			if(minCount > 0) {
				meanMinTemp = minSum/minCount;
			}
			
			if(maxCount > 0) {
				meanMaxTemp = maxSum/maxCount;
			}
			
			result.append("(");
			result.append(y);
			result.append(",");
			result.append(meanMinTemp);
			result.append(",");
			result.append(meanMaxTemp);
			result.append(")");
			
			result.append("]");
			context.write(NullWritable.get(), new Text(result.toString()));
		}
	}
	
	
	  /***
	   * Main : Setups up the mapreduce environment configuration.
	   * This program involves only Mapper and Reducer. The Map output key is stationID, 
	   * output value is of type StationInfo.
	   * The output key if NullWritable object and the value is a Text object
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
	    job.setJarByClass(TimeSeries.class);
	    job.setMapperClass(SecondarySortMapper.class);
	
	    job.setPartitionerClass(KeyPartitioner.class);
	    job.setSortComparatorClass(KeyComparator.class);
	    job.setGroupingComparatorClass(GroupComparator.class);

	    job.setReducerClass(SecondarySortReducer.class);
	    
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		    
		job.setMapOutputKeyClass(StationKey.class);
		job.setMapOutputValueClass(StationInfo.class);
		    
		for (int i = 0; i < otherArgs.length - 1; ++i) {
		     FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		
		File file = new File(otherArgs[0]);
		String path = file.getParent();
		String name = file.getName();
		
		/**
		 * Read all input files 1880.csv, 1881.csv, 1882.csv, 1883.csv,...1889.csv
		 * by matching the pattern for file name.
		 */
		/*for(int i=1;i<10;i++) {
			FileInputFormat.addInputPath(job, new Path("s3://no-combiner-bucket/input"+"/"+name.substring(0,3)+i+".csv"));
		}*/
		
		FileInputFormat.addInputPath(job, new Path("s3://no-combiner-bucket/input/1880.csv"));
		FileInputFormat.addInputPath(job, new Path("s3://no-combiner-bucket/input/1881.csv"));
		FileInputFormat.addInputPath(job, new Path("s3://no-combiner-bucket/input/1882.csv"));
		FileInputFormat.addInputPath(job, new Path("s3://no-combiner-bucket/input/1883.csv"));
		FileInputFormat.addInputPath(job, new Path("s3://no-combiner-bucket/input/1884.csv"));
		FileInputFormat.addInputPath(job, new Path("s3://no-combiner-bucket/input/1885.csv"));
		FileInputFormat.addInputPath(job, new Path("s3://no-combiner-bucket/input/1886.csv"));
		FileInputFormat.addInputPath(job, new Path("s3://no-combiner-bucket/input/1887.csv"));
		FileInputFormat.addInputPath(job, new Path("s3://no-combiner-bucket/input/1888.csv"));
		FileInputFormat.addInputPath(job, new Path("s3://no-combiner-bucket/input/1889.csv"));
		
	    FileOutputFormat.setOutputPath(job,
		   new Path(otherArgs[otherArgs.length - 1]));
		   System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
