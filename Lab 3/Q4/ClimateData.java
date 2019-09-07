import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//Problem No.4
public class ClimateData extends Configured implements Tool
{
	public static class ClimateDataMapper extends Mapper<LongWritable, Text, Year, IntWritable>
	{

		private IntWritable temp = new IntWritable(0);
		private Year yearKey = new Year();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			Integer year = new Integer(Integer.parseInt(value.toString().substring(15, 19)));
			temp = new IntWritable(Integer.parseInt(value.toString().substring(87, 92)));
			//System.out.println(value.toString().substring(15, 19));
			yearKey.set(year);
			context.write(yearKey, temp);
		}
	}

	public static class ClimateDataReducer extends Reducer<Year, IntWritable, Year, DoubleWritable>
	{
		private DoubleWritable result = new DoubleWritable();
		private int count=0;
		
		@Override
		public void reduce(Year key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			int cnt = 0;
			for(IntWritable val: values){
				sum+=val.get();
				cnt++;
//				System.out.println(key+":"+val);
			}
			result.set(sum/cnt/10.00);
			context.write(key, result);
			count++;
		}
		
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new ClimateData(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{
		Configuration c = getConf();
		// configuration should contain reference to your namenode
		FileSystem fs = FileSystem.get(c);
		// true stands for recursively deleting the folder you gave
		if(fs.exists(new Path(args[2])))
			fs.delete(new Path(args[2]), true);
		
		Job job = new Job(c, "ClimateDataJob");
		job.setJarByClass(ClimateData.class);

		job.setMapperClass(ClimateDataMapper.class);
		job.setReducerClass(ClimateDataReducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(Year.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
