import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class ReduceSideJoin
{
	public static class ReduceSideJoinMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		boolean fileA = true;
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{

			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			Text outputKey = new Text();
			Text outputValue = new Text();
			String category = "";
			String o_value = "";
			if( fileA ) {
				String ID = itr.nextToken();
				String price = itr.nextToken();
				category = itr.nextToken();
				o_value = "A|" + ID + "|" + price;
			}
			else {
				category = itr.nextToken();
				String category_name = itr.nextToken();
				o_value = "B|" + category_name;
			}
			outputKey.set(category);
			outputValue.set( o_value );
			context.write( outputKey, outputValue );
		}

		protected void setup(Context context) throws IOException, InterruptedException
		{
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			if ( filename.indexOf( "fileA" ) != -1 ) fileA = true;
			else fileA = false;
		}
	}

	public static class ReduceSideJoinReducer extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {		
			Text reduce_key = new Text();
			Text reduce_result = new Text();
			String category_name = "";
			ArrayList<String> buffer = new ArrayList<String>();
			for (Text val : values) {
				StringTokenizer itr = new StringTokenizer(val.toString(), "|");
				String file_type = itr.nextToken();
				if(file_type.equals("B")) {
					category_name = itr.nextToken();
				}
				else{
					if(category_name.length() == 0){
						buffer.add(val.toString());
					}
					else{
						String ID = itr.nextToken();
						String price = itr.nextToken();
						reduce_key.set(ID);
						reduce_result.set(price + "\t" + category_name);
						context.write(reduce_key, reduce_result);
					}
				}
			}
			
			for(int i = 0; i < buffer.size(); i++){
				StringTokenizer itr = new StringTokenizer(buffer.get(i), "|");
				String file_type = itr.nextToken();
				String ID = itr.nextToken();
				String price = itr.nextToken();
				reduce_key.set(ID);
				reduce_result.set(price + "\t" + category_name);
				context.write(reduce_key, reduce_result);
			}
		}
	}

	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "ReduceSideJoin");
		job.setJarByClass(ReduceSideJoin.class);
		job.setMapperClass(ReduceSideJoinMapper.class);
		job.setReducerClass(ReduceSideJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

