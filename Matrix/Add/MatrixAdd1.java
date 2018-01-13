import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class MatrixAdd1
{
	public static class MatrixAddMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable i_value = new IntWritable(1);
		private Text word = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			int row_id = Integer.parseInt(itr.nextToken().trim());
			int col_id = Integer.parseInt(itr.nextToken().trim());
			int m_value = Integer.parseInt(itr.nextToken().trim());
			
			word.set(row_id + ", " + col_id);
			i_value.set(m_value);
			context.write(word, i_value);	
		}
	}

	public static class MatrixAddReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "matrixAdd");

		job.setJarByClass(MatrixAdd1.class);
		job.setMapperClass(MatrixAddMapper.class);
		job.setReducerClass(MatrixAddReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(args[1]), true);
		job.waitForCompletion(true);
	}
}
