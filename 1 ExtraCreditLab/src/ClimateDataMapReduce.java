import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//ExtraCredit Question
public class ClimateDataMapReduce extends Configured implements Tool
{
	public static class ClimateDataMapper extends Mapper<LongWritable, Text, CompositeKey, Text>
	{

		private CompositeKey compKey = new CompositeKey();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			compKey.setStationId(value.toString().substring(4,10)+"-"+value.toString().substring(10,15));
			compKey.setTemp(Integer.parseInt(value.toString().substring(87, 92)));
			Text year = new Text(value.toString().substring(15, 19));
			context.write(compKey, year);
		}
		
	}

	public static class ClimateDataReducer extends Reducer<CompositeKey, Text, CompositeKey, Text>
	{
		private Text result = new Text();
		
		@Override
		public void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			for(Text t:values){
				result.set(key.getTemp()+"\t"+t);
				context.write(key, result);
			}
		}
		
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new ClimateDataMapReduce(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{
		Configuration c = getConf();
		c.set("mapreduce.output.basename", args[3]);
		// configuration should contain reference to your namenode
		FileSystem fs = FileSystem.get(c);
		// true stands for recursively deleting the folder you gave
		if(fs.exists(new Path(args[2])))
			fs.delete(new Path(args[2]), true);
		
		Job job = new Job(c, "ClimateDataJob");
		job.setJarByClass(ClimateDataMapReduce.class);

		job.setMapperClass(ClimateDataMapper.class);
		job.setReducerClass(ClimateDataReducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(CompositeKey.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
