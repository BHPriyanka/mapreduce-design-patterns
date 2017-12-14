package noCombiner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/***
 * WeatherTempWritable: Value type class used by MinMaxTempNoCombiner class.
 * Object of this class is passed as the output value from the mapper function.
 * Contains a Text object to indicate whether the record is a max or min temperature record
 * and the value of the temperature as a DoubleWritable type.
 */
public class WeatherTempWritable implements Writable{
	Text type;
	DoubleWritable tempVal;
	
	WeatherTempWritable(Text isMinTemp, DoubleWritable tempVal){
		this.type = isMinTemp;
		this.tempVal = tempVal;
	}
	
	WeatherTempWritable(){
		set(new Text(), new DoubleWritable());
	}
	
	/*
	 * getters for the type and temperature value
	 */
    public Text getType() {
    	return this.type;
    }
    
    public DoubleWritable getTempVal() {
    	return this.tempVal;
    }
    
    /*
     * setter for the type and temperature values
     */
    public void set(Text type, DoubleWritable val) {
    	this.type = type;
    	this.tempVal = val;
    }
    
    //@Override
    public void readFields(DataInput in) throws IOException {
       type.readFields(in);
       tempVal.readFields(in); 
    }
 
    //@Override
    public void write(DataOutput out) throws IOException {
        type.write(out);
        tempVal.write(out);
    }
    
    @Override
    public String toString() {
        return type + ";" + tempVal;
    }

}
