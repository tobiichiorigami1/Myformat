package Myformat;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MyFormat extends TextOutputFormat<Text,NullWritable>{

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(
			TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration con=arg0.getConfiguration();
		FileSystem fs=FileSystem.get(con);
		FSDataOutputStream ds1=fs.create(new Path("hdfs:///user/root/input1"),true);
		FSDataOutputStream ds2=fs.create(new Path("hdfs:///user/root/input2"),true);
		return new MyRWrite(ds1, ds2);
	}
	public static class MyRWrite extends RecordWriter<Text,NullWritable>{

		
		FSDataOutputStream fs1=null;
		FSDataOutputStream fs2=null;
		
		public MyRWrite(FSDataOutputStream fs1, FSDataOutputStream fs2) {
			super();
			this.fs1 = fs1;
			this.fs2 = fs2;
		}

		@Override
		public void close(TaskAttemptContext arg0) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void write(Text arg0, NullWritable arg1) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
		      if(arg0.toString().split(":")[0].equals("1")){
		    	  fs1.writeBytes(arg0.toString().split(":")[1]+"\n");
		      }	
		      else {
		    	  fs2.writeBytes(arg0.toString().split(":")[1]+"\n");
			}
		
		}
    	
    }
}

    





