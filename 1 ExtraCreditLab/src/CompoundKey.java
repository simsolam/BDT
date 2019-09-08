import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class CompoundKey implements WritableComparable {
	Text stationId = new Text();
	IntWritable temp = new IntWritable();
	
	CompoundKey(){}
	
	CompoundKey(String station, Integer temp){
		this.stationId.set(station);
		this.temp.set(temp);
	}
	
	public Text getStationId(){
		return stationId;
	}
	
	public void setStationId(String station){
		this.stationId.set(station);
	}
	
	public IntWritable getTemp(){
		return temp;
	}
	
	public void setTemp(Integer temp){
		this.temp.set(temp);
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		stationId.readFields(arg0);
		temp.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		stationId.write(arg0);
		temp.write(arg0);
	}

	@Override
	public int compareTo(Object o) {
		if(!(o instanceof CompoundKey))
			return -1;
		CompoundKey compKey = (CompoundKey)o;
		if(this.stationId.compareTo(compKey.stationId)==0){
			return -1*this.temp.compareTo(compKey.temp);
		}
		
		return this.stationId.compareTo(compKey.stationId);
	}
	
	@Override
	public String toString(){
		return stationId.toString();
	}
	
}
