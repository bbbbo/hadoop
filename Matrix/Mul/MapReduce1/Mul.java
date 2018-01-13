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

public class Mul
{
	public static class DoubleString implements WritableComparable
	{
		String joinKey =  new String();
		String x = new String();
	
		public DoubleString() { };
		
		public DoubleString(String joinKey, String x)
		{
			this.joinKey = joinKey;
			this.x = x;
		}
		
		public void readFields(DataInput in) throws IOException
		{
			joinKey = in.readUTF();
			x = in.readUTF();
		}

		public void write(DataOutput out) throws IOException
		{
			out.writeUTF(joinKey);
			out.writeUTF(x);
		}

		public int compareTo(Object o1)
		{
			DoubleString o = (DoubleString) o1;
			int ret = joinKey.compareTo(o.joinKey);
			if(ret != 0)	return ret;
			return -1*x.compareTo(o.x);
		}

		public String toString(){
			return  joinKey + "," + x;
		}
	}

		
	public static class CompositeKeyComparator extends WritableComparator
	{
		protected CompositeKeyComparator() {
			super(DoubleString.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2){
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;
			int result = k1.joinKey.compareTo(k2.joinKey);
			if(result == 0){
				result = -1*k1.x.compareTo(k2.x);
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


	public static class FirstGroupingComparator extends WritableComparator 
	{
		protected FirstGroupingComparator(){
			super(DoubleString.class, true);
		}
		
		public int compare(WritableComparable w1, WritableComparable w2){
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;
			return k1.joinKey.compareTo(k2.joinKey);
		}
	}

	public static class MulMapper extends Mapper<Object, Text, DoubleString, Text>
	{
		private int m_value;
		private int k_value;
		private int n_value;

		boolean isA = false;
		boolean isB = false;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			Text outputValue = new Text();
			StringTokenizer itr = new StringTokenizer(value.toString());
			String row_id = itr.nextToken();
			String col_id = itr.nextToken();
			int matrix_value = Integer.parseInt(itr.nextToken());
			
			DoubleString doubleString = null;
			if(isA){
				for(int i = 0; i < n_value; i++){
					String joinKey = row_id + "," + i;
					doubleString = new DoubleString(joinKey, col_id);
					outputValue.set("A|" + matrix_value);
					context.write(doubleString, outputValue); 
				}
			}else if(isB){
				for(int i = 0; i < m_value; i++){
					String joinKey = i + "," + col_id;
					doubleString = new DoubleString(joinKey, row_id);
					outputValue.set("B|" + matrix_value);
					context.write(doubleString, outputValue);
				}
			}

		}
		
		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();

			m_value = conf.getInt("m", -1);
			k_value = conf.getInt("k", -1);
			n_value = conf.getInt("n", -1);

			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			if(filename.indexOf("matrix_a") != -1)
				isA = true;
			if(filename.indexOf("matrix_b") != -1)
				isB = true;
		}
	}

	public static class MulReducer extends Reducer<DoubleString, Text, Text, IntWritable>
	{
		private Text outputKey = new Text();
		private IntWritable outputValue = new IntWritable();
		public void reduce(DoubleString key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			StringTokenizer st = new StringTokenizer(key.toString(), ",");
			String row = st.nextToken();
			String col = st.nextToken();
			outputKey.set(row + "," + col);

			ArrayList<String> a = new ArrayList<String>();
			ArrayList<String> b = new ArrayList<String>();
			for(Text val : values){
				StringTokenizer itr = new StringTokenizer(val.toString(), "|");		
				String matrixName = itr.nextToken();
				
				if(matrixName.equals("A")){
					a.add(itr.nextToken());
				}else{
					b.add(itr.nextToken());
				}
			}
			
			int[] mul = new int[a.size()];
			for(int i = 0; i < a.size(); i++){
				mul[i] = Integer.parseInt(a.get(i)) * Integer.parseInt(b.get(i));
			}
			
			int sum = 0;
			for(int i = 0; i < a.size(); i++){
				sum += mul[i];
			}

			outputValue.set(sum);
			context.write(outputKey, outputValue);
		}
	}


	 public static void main(String[] args) throws Exception {
        	int m_value = 2;
                int k_value = 2;
                int n_value = 2;

                Configuration conf = new Configuration();
                String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

                if(otherArgs.length != 2) {
                        System.err.println("Usage: MatrixMul <in> <out>");
                        System.exit(2);
                }
                conf.setInt("m", m_value);
                conf.setInt("k", k_value);
                conf.setInt("n", n_value);  
	
                Job job = new Job(conf, "Mul");
                job.setJarByClass(Mul.class);
                job.setMapperClass(MulMapper.class);
                job.setReducerClass(MulReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
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
