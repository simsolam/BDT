import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

//Creating a composite key with StationId and Temperature
public class CompositeKey implements WritableComparable {
	Text stationId = new Text();
	IntWritable temp = new IntWritable();
	
	CompositeKey(){}
	
	CompositeKey(String station, Integer temp){
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
		if(!(o instanceof CompositeKey))
			return -1;
		CompositeKey compKey = (CompositeKey)o;
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
