import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class Year implements WritableComparable {
	private IntWritable yr = new IntWritable();
	
	Year(){}
	
	Year(Integer yy){
		this.yr = new IntWritable(yy);
	}
	
	public Integer getInt(){
		return yr.get();
	}
	
	public IntWritable get(){
		return yr;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		yr.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		yr.write(arg0);
	}

	@Override
	public int compareTo(Object o) {
		return -1*yr.compareTo(((Year)o).get());
	}

	public void set(Integer year) {
		this.yr.set(year);
	}
	
	@Override
	public String toString(){
		return yr.toString();
	}

}
