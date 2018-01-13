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

public class ReduceSideJoin2
{
	public static class DoubleString implements WritableComparable
	{
		String joinKey = new String();
		String tableName = new String();
		
		public DoubleString() {};

		public DoubleString( String _joinKey, String _tableName )
		{
			joinKey = _joinKey;
			tableName = _tableName;
		}

		public void readFields(DataInput in) throws IOException
		{
			joinKey = in.readUTF();
			tableName = in.readUTF();
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(joinKey);
			out.writeUTF(tableName);
		}
		
		public int compareTo(Object o1)
		{	
			DoubleString o = (DoubleString) o1;
			int ret = joinKey.compareTo( o.joinKey );
			if (ret!=0) return ret;
			return -1*tableName.compareTo( o.tableName);
 		}

		public String toString() { return joinKey + " " + tableName; }
	}


	public static class CompositeKeyComparator extends WritableComparator {
		protected CompositeKeyComparator() {
			super(DoubleString.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;
			int result = k1.joinKey.compareTo(k2.joinKey);

			if(0 == result) {
				result = -1* k1.tableName.compareTo(k2.tableName);
			}

			return result;
		}
 	}

	public static class FirstPartitioner extends Partitioner<DoubleString, Text>
	{
		public int getPartition(DoubleString key, Text value, int numPartition)
		{
			return key.joinKey.hashCode()%numPartition;
		}
	}

	public static class FirstGroupingComparator extends WritableComparator {
		protected FirstGroupingComparator()
		{
			super(DoubleString.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;

 			return k1.joinKey.compareTo(k2.joinKey);	
		}
	}

	public static class ReduceSideJoin2Mapper extends Mapper<Object, Text, DoubleString, Text>
	{
		boolean fileA = true;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
 		{
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			Text outputValue = new Text();
			String category = "";
			String o_value = "";
			DoubleString doubleString = null;

			if( fileA ) {
                                String ID = itr.nextToken();
                                String price = itr.nextToken();
                                category = itr.nextToken();
                                o_value = "A|" + ID + "|" + price;
				doubleString = new DoubleString(category, "A");
                        }
                        else {
                                category = itr.nextToken();
                                String category_name = itr.nextToken();
                                o_value = "B|" + category_name;
				doubleString = new DoubleString(category, "B");
                        }

                        outputValue.set( o_value );
                        context.write( doubleString, outputValue );
                }

                protected void setup(Context context) throws IOException, InterruptedException
                {
                        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
                        if ( filename.indexOf( "fileA" ) != -1 ) fileA = true;
                        else fileA = false;
                }
	}

	public static class ReduceSideJoin2Reducer extends Reducer<DoubleString, Text, Text, Text>
	{
		public void reduce(DoubleString key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			Text reduce_key = new Text();
			Text reduce_val = new Text();
			
			String category_name = "";
			for(Text val : values){
				StringTokenizer itr = new StringTokenizer(val.toString(), "|");
				String file_type = itr.nextToken();
				
				if(file_type.equals("B")) {
					category_name = itr.nextToken();
				}else{
					String ID = itr.nextToken();
					String price = itr.nextToken();
					reduce_key.set(ID);
					reduce_val.set(price + " " + category_name);
					context.write(reduce_key, reduce_val);
				}
			}	
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: ReduceSideJoin2 <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "ReduceSideJoin2");
		job.setJarByClass(ReduceSideJoin2.class);
		job.setMapperClass(ReduceSideJoin2Mapper.class);
		job.setReducerClass(ReduceSideJoin2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(DoubleString.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}






















