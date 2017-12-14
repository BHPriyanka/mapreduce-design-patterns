package secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/***
 * StationInfo: Value type class used by TimeSeries class.
 * Object of this class is passed as the output value from the mapper function.
 * Contains placeholders for running sum of max and min temperatures, and counts for
 * max and min temperatures and the corresponding year.
 */
public class StationInfo implements Writable{
	long maxSum;
	int maxCount;
	long minSum;
	int minCount;
	int year;
	
	StationInfo(long maxSum,
			int maxCount,long minSum, int minCount, int year){
		this.maxSum = maxSum;
		this.maxCount = maxCount;
		this.minSum = minSum;
		this.minCount = minCount;
		this.year = year;
	}
	
	StationInfo(){
		set(0,0,0,0,0);
	}
    
	/*
	 * getMaxSum, getMinSum, getMaxCount, getMinCount, getYear:
	 * getters for maxSum, minSum, maxCount, year and minCount fields
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
    
    public int getYear() {
    	return this.year;
    }
    
    /* setter used by the default constructor when no parameters are specified.
     */
    public void set(long maxSum, int maxCount, long minSum, int minCount, int year) {
    	this.maxCount = maxCount;
    	this.maxSum = maxSum;
    	this.minCount = minCount;
    	this.minSum = minSum;
    	this.year = year;
    }
    
    //@Override
    public void readFields(DataInput in) throws IOException {
       maxSum = in.readLong();
       maxCount = in.readInt();
       minSum = in.readLong();
       minCount = in.readInt();
       year = in.readInt();
    }
 
    //@Override
    public void write(DataOutput out) throws IOException {
       
        out.writeLong(maxSum);
        out.writeInt(maxCount);
        out.writeLong(minSum);
        out.writeInt(minCount);
        out.writeInt(year);
    }
    
    @Override
    public String toString() {
        return maxSum+";"+maxCount+";"+minSum+";"+minCount+";"+year;
    }
}
