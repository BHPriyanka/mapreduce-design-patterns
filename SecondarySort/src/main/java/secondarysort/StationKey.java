package secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/***
 * StationKey: Composite key class used by TimeSeries class.
 * Object of this class is passed as the output key from the mapper function.
 * Contains placeholders for year and the stationID
 */
public class StationKey implements WritableComparable<StationKey>{
	IntWritable year;
	Text stationID;
		
	StationKey(){	
		set(new Text(), new IntWritable());
	}
	
	StationKey(Text id, IntWritable y) {
		this.year=y;
		this.stationID = id;
	}
	
	/* setter used by th default cosntructor*/
	public void set(Text id, IntWritable y) {
		this.stationID = id;
		this.year = y;
	}
	
	/*
	 * getYear, getStationID: getters to fetch the composit key values
	 */
	public int getYear() {
		return this.year.get();
	}
	
	public String getStationID() {
		return this.stationID.toString();
	}
	
	/*
	 * setYear and setStationID: setters to initialize the values of variables
	 */
	public void setYear(IntWritable y) {
		this.year = y;
	}
	
	public void setStationID(Text id) {
		this.stationID = id;
	}
	
	
	 //@Override
	 public void readFields(DataInput in) throws IOException {
     	 stationID.readFields(in);
	     year.readFields(in);
	 }

	 //@Override
	 public void write(DataOutput out) throws IOException {
	     stationID.write(out);
	     year.write(out);
	 }
	
	/**
	 * compare: compares the values of two years.
	 *         - returns 0 if they are same
	 *         - returns -1 if y1<y2
	 *         - returns 1 if y1>y2
	 * @param y1: the value of the year to be compared with
	 * @param y2: the value of the year to be compared
	 */
	public int compare(int y1, int y2) {
		if(y1==y2) {
			return 0;
		} else if(y1 < y2) {
			return -1;
		}
		return 1;
		
	}

	/*
	 * compareTo: overridden method of WritableComparable class to compare the StationKeys
	 *          - compares it against the object invoking this method.
	 *          - first compares the stationIDs of two keys, if they are not same, returns
	 *          - if they are same, proceed to compare the year of both the keys.
	 *          - returns an integer value of 0,-1 or 1
	 * @param k2: the stationKey to be compared
	 */
	public int compareTo(StationKey k2) {
		int cmp = stationID.toString().compareTo(k2.getStationID());
		
    	if (cmp != 0) {
    		return cmp;
    	}
    	return year.compareTo(new IntWritable(k2.getYear()));
	}
}