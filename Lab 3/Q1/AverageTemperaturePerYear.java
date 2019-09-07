package lab3_1;



import java.io.IOException;

import org.apache.commons.math3.stat.descriptive.StatisticalSummaryValues;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class AverageTemperaturePerYear {
	
	public static class AverageMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		public static final int MISSING = 9999;
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
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
             context.write(new Text(year),new IntWritable(temperature)); 
			
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
	   job.setMapOutputValueClass(IntWritable.class);
	   
       
        
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);
        
       System.exit(job.waitForCompletion(true)?0:1);      

		
	}
	

	
	public static class AverageReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			int max_temp = 0; 
            int count = 0;
            for (IntWritable value : values)
                        {
                                    max_temp += value.get();     
                                    count+=1;
                        }
            
            context.write(key, new IntWritable(max_temp/count));
            }
			
		}
	}



