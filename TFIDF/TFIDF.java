import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class TFIDF
{
        public static class TFMapper1 extends Mapper<Object, Text, Text, LongWritable>
        {
                private String docname;
                private final static LongWritable one = new LongWritable(1);
                private Text outputKey = new Text();

                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        StringTokenizer itr = new StringTokenizer(value.toString());
                        while(itr.hasMoreTokens()) {
                                String word = itr.nextToken().toLowerCase();
                                outputKey.set(word + "," + docname);
                                context.write(outputKey, one);
                        }
                }

                protected void setup(Context context) throws IOException, InterruptedException {
                        docname = ((FileSplit) context.getInputSplit()).getPath().getName();
                }
        }

        public static class TFReducer1 extends Reducer<Text, LongWritable, Text, LongWritable>
        {
                private LongWritable outputValue = new LongWritable();

                public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
                        long sum = 0;
                        for(LongWritable val : values) {
                                sum += val.get();
                        }
                        outputValue.set(sum);
                        context.write(key, outputValue);
                }
        }

	public static class TFMapper2 extends Mapper<Object, Text, Text, Text>
        {
                private Text outputKey = new Text();
		private Text outputValue = new Text();

                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			String i_key = itr.nextToken();
			String n = itr.nextToken();

			StringTokenizer st = new StringTokenizer(i_key, ",");
			String word = st.nextToken();
			String docname = st.nextToken();

			outputKey.set(docname);
			outputValue.set(word + "," + n);
			context.write(outputKey, outputValue);
		}
	}

	public static class TFReducer2 extends Reducer<Text, Text, Text, Text>
        {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
              	
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<String> buffer = new ArrayList<String>();
			String word = "";
			String n = "";
			int N = 0;
			for(Text val : values) {
				buffer.add(val.toString());
				StringTokenizer itr = new StringTokenizer(val.toString(), ",");
				word = itr.nextToken();
				n = itr.nextToken();

				N += Integer.parseInt(n);
			}
			
			for(int i = 0; i < buffer.size(); i++){
				StringTokenizer st = new StringTokenizer(buffer.get(i), ",");
				word = st.nextToken();
				n = st.nextToken();

				outputKey.set(word + "," + key);
                    		outputValue.set(n + "," + N);
                       	 	context.write(outputKey, outputValue);
			}
                }
        }

	public static class TFMapper3 extends Mapper<Object, Text, Text, Text>
        {
                private Text outputKey = new Text();
                private Text outputValue = new Text();

                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        StringTokenizer itr = new StringTokenizer(value.toString());
                        String i_key = itr.nextToken();
                        String i_value = itr.nextToken();

                        StringTokenizer st = new StringTokenizer(i_key, ",");
                        String word = st.nextToken();
                        String docname = st.nextToken();

			StringTokenizer st2 = new StringTokenizer(i_value, ",");
			String n = st2.nextToken();
			String N = st2.nextToken();
			String one = "1";

                        outputKey.set(word);
                        outputValue.set(docname + "," + n + "," + N + "," + one);
                        context.write(outputKey, outputValue);
                }

        }

	public static class TFReducer3 extends Reducer<Text, Text, Text, Text>
        {
                private Text outputKey = new Text();
                private Text outputValue = new Text();

                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                        ArrayList<String> buffer = new ArrayList<String>();
                        int cnt = 0;
                        for(Text val : values) {
				buffer.add(val.toString());
				String[] str = val.toString().split(",");
				cnt += Integer.parseInt(str[3]);
			}

			for(int i = 0; i < buffer.size(); i++){
				StringTokenizer itr = new StringTokenizer(buffer.get(i), ",");
				String docname = itr.nextToken();
				String n = itr.nextToken();
				String N = itr.nextToken();
				
				outputKey.set(key + "," + docname);
				outputValue.set(n + "," + N + "," + cnt);
				context.write(outputKey, outputValue);
			}
		}
	}

	public static class TFMapper4 extends Mapper<Object, Text, Text, DoubleWritable>
        {
                private Text outputKey = new Text();
                private DoubleWritable outputValue = new DoubleWritable();
		private int numOfDocs = 0;

                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        StringTokenizer itr = new StringTokenizer(value.toString());
			String i_key = itr.nextToken();
			String i_value = itr.nextToken();
			
			StringTokenizer st = new StringTokenizer(i_value, ",");
			double n = Double.parseDouble(st.nextToken());
			double N = Double.parseDouble(st.nextToken());
			double m = Double.parseDouble(st.nextToken());
			double D = Math.abs((double)numOfDocs);

			double result = (n/N) * Math.log((D/m));
			
			outputKey.set(i_key);
			outputValue.set(result);
			context.write(outputKey, outputValue);
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			numOfDocs = conf.getInt("NumOfDocs", -1);
		}
	}

        public static void main(String[] args) throws Exception {
                String result1 = "/TF1";
                String result2 = "/TF2";
                String result3 = "/TF3";

                Configuration conf = new Configuration();	
		int NumOfDocs = 4;
		conf.setInt("NumOfDocs", NumOfDocs);

                Job job1 = new Job(conf, "TFIDF1");
                job1.setJarByClass(TFIDF.class);
                job1.setMapperClass(TFMapper1.class);
                job1.setReducerClass(TFReducer1.class);
                job1.setOutputKeyClass(Text.class);
                job1.setOutputValueClass(LongWritable.class);
	        FileInputFormat.addInputPath(job1, new Path(args[0]));
                FileOutputFormat.setOutputPath(job1, new Path(result1));
                FileSystem.get(job1.getConfiguration()).delete(new Path(result1), true);
                job1.waitForCompletion(true);

		
		Job job2 = new Job(conf, "TFIDF2");
                job2.setJarByClass(TFIDF.class);
                job2.setMapperClass(TFMapper2.class);
                job2.setReducerClass(TFReducer2.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job2, new Path(result1));
                FileOutputFormat.setOutputPath(job2, new Path(result2));
                FileSystem.get(job2.getConfiguration()).delete(new Path(result2), true);
                job2.waitForCompletion(true);
		

		Job job3 = new Job(conf, "TFIDF3");
                job3.setJarByClass(TFIDF.class);
                job3.setMapperClass(TFMapper3.class);
                job3.setReducerClass(TFReducer3.class);
                job3.setOutputKeyClass(Text.class);
                job3.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job3, new Path(result2));
                FileOutputFormat.setOutputPath(job3, new Path(result3));
                FileSystem.get(job3.getConfiguration()).delete(new Path(result3), true);
                job3.waitForCompletion(true);

		Job job4 = new Job(conf, "TFIDF4");
                job4.setJarByClass(MapSideJoin.class);
                job4.setMapperClass(TFMapper4.class);
                job4.setNumReduceTasks(0);
                job4.setMapOutputKeyClass(Text.class);
                job4.setMapOutputValueClass(DoubleWritable.class);
                FileInputFormat.addInputPath(job4, new Path(result3));
                FileOutputFormat.setOutputPath(job4, new Path(args[1]));
                FileSystem.get(job4.getConfiguration()).delete( new Path(args[1]), true);
                System.exit(job4.waitForCompletion(true) ? 0 : 1);

        }
}

