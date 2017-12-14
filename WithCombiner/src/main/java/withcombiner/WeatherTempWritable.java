package withcombiner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/***
 * WeatherTempWritable: Value type class used by MinMaxWithCombiner class.
 * Object of this class is passed as the output value from the mapper function.
 * Contains placeholders for running sum of max and min temperatures, and counts for
 * max and min temperatures.
 */
public class WeatherTempWritable implements Writable{
	long maxSum;
	int maxCount;
	long minSum;
	int minCount;
	
	WeatherTempWritable(long maxSum,
			int maxCount,long minSum, int minCount){
		this.maxSum = maxSum;
		this.maxCount = maxCount;
		this.minSum = minSum;
		this.minCount = minCount;
	}
	
	WeatherTempWritable(){
		set(0,0,0,0);
	}
	
	/*
	 * getMaxSum, getMinSum, getMaxCount, getMinCount:
	 * getters for maxSum, minSum, maxCount and minCount fields
	 */
    public long getMaxSum() {
    	return this.maxSum;
    }
    
    public long getMinSum() {
    	return this.minSum;
    }
    
    public int getMaxCount() {
    	return this.maxCount;
    }
    
    public int getMinCount() {
    	return this.minCount;
    }
    
    /* setter used by the default constructor when no parameters are specified.
     */
    public void set(long maxSum, int maxCount, long minSum, int minCount) {
    	this.maxCount = maxCount;
    	this.maxSum = maxSum;
    	this.minCount = minCount;
    	this.minSum = minSum;
    }
    
    //@Override
    public void readFields(DataInput in) throws IOException {
       maxSum = in.readLong();
       maxCount = in.readInt();
       minSum = in.readLong();
       minCount = in.readInt();
    }
 
    //@Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(maxSum);
        out.writeInt(maxCount);
        out.writeLong(minSum);
        out.writeInt(minCount);
    }
    
    @Override
    public String toString() {
        return maxSum+";"+maxCount+";"+minSum+";"+minCount;
    }

}
