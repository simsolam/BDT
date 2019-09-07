package lab3_1;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.math3.stat.descriptive.StatisticalSummaryValues;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/*With a combiner only*/

public class AverageTemperaturePerYear {
	
	public static class AverageMapper extends Mapper<Object, Text, Text, Pair>{
		
		public static final int MISSING = 9999;
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, Pair>.Context context)
				throws IOException, InterruptedException {
			
			 String line = value.toString();
             String year = line.substring(15,19);               
             int temperature;                    
             if (line.charAt(87)=='+')
                         temperature = Integer.parseInt(line.substring(88, 92));
             else
                         temperature = Integer.parseInt(line.substring(87, 92));       
            
             String quality = line.substring(92, 93);
             if(temperature != MISSING && quality.matches("[01459]"))
             context.write(new Text(year),new Pair(temperature,1)); 
			
		}
	}
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		
		 if (args.length != 2)
         {
                System.err.println("Please Enter the input and output parameters");
                System.exit(-1);
         }
        
       Job job = new Job();
       
       job.setJarByClass(AverageTemperaturePerYear.class);
       job.setMapperClass(AverageMapper.class);
       job.setCombinerClass(AverageCombiner.class);
       job.setReducerClass(AverageReducer.class);
      
       
       
       job.setJobName("Average temperature");
       
       Configuration conf = new Configuration();
		//Creating FileSystem object with the configuration
		FileSystem fs = FileSystem.get(conf);
		//Check if output path (args[1])exist or not
		if(fs.exists(new Path(args[1]))){
		   /*If exist delete the output path*/
		   fs.delete(new Path(args[1]),true);
		}
        
       FileInputFormat.addInputPath(job, new Path(args[0]));		
	   FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
	   job.setMapOutputKeyClass(Text.class);
	   job.setMapOutputValueClass(Pair.class);
	          
        
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);
        
       System.exit(job.waitForCompletion(true)?0:1);      

		
	}
	

	public static class AverageCombiner extends Reducer<Text, Pair, Text, Pair>{
		@Override
		protected void reduce(Text key, Iterable<Pair> values,
				Reducer<Text, Pair, Text, Pair>.Context context)
				throws IOException, InterruptedException {
			
			int sum = 0; 
            int count = 0;
            for (Pair value : values)
                        {
                                    sum += value.getSum();     
                                    count+=value.getCount();
                        }
            
            context.write(key, new Pair(sum,count));
            
			
		}
	}
	
	public static class AverageReducer extends Reducer<Text, Pair, Text, IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<Pair> values,
				Reducer<Text, Pair, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			int sum = 0; 
            int count = 0;
            for (Pair value : values)
                        {
                                    sum += value.getSum();     
                                    count+=value.getCount();
                        }
            
            context.write(key, new IntWritable(sum/count));
            }
			
		}
	
	public static class Pair implements Writable {
		
		private int sum;
		private int count;
		
		public Pair(){}
		
		public Pair(int sum, int count) {
			
			this.sum = sum;
			this.count = count;
		}
		
		public int getSum() {
			return sum;
		}
		public void setSum(int sum) {
			this.sum = sum;
		}
		public int getCount() {
			return count;
		}
		public void setCount(int count) {
			this.count = count;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			
			sum=in.readInt();
			count=in.readInt();
			
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			
			out.writeInt(sum);
			out.writeInt(count);
			
		}
	}
}



