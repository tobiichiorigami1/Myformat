package Myformat;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class JoinandWrite extends Configured implements Tool{

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static class JobMapper extends Mapper<LongWritable,Text,Text,NullWritable>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String split[]=value.toString().split(" ");
                   if(split.length>=6){
                	   context.write(new Text("1:"+value.toString()),NullWritable.get());
                   }	
                   else {
                	   context.write(new Text("2:"+value.toString()),NullWritable.get());
				}
		}
	}
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
         ToolRunner.run(new JoinandWrite(), args);
	}

	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
//		URI uri=new URI("file:///user/input/1.txt");
//		job.addCacheFile(uri);
		job.setJarByClass(JoinandWrite.class);
		job.setMapperClass(JobMapper.class);
		job.setOutputFormatClass(MyFormat.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path("file:///user/input"));
		FileOutputFormat.setOutputPath(job,new Path("hdfs:///user/output"));
		return job.waitForCompletion(true)?1:0;
	}

}
