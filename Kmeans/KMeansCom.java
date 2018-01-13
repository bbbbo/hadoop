import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class KMeansCom
{
	private static double[] center_x;
	private static double[] center_y;
	private static int n_centers;

	public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		private IntWritable one_key = new IntWritable();

		public double getDist(double x1, double y1, double x2, double y2)
		{
			double dist = (x1-x2)* (x1-x2) + (y1-y2)*(y1-y2);
			return Math.sqrt( dist );
		}

		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
		{
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			if( itr.countTokens() < 2 ) return;
			if( n_centers == 0 ) return;

			double x = Double.parseDouble(itr.nextToken().trim());
			double y = Double.parseDouble(itr.nextToken().trim());
			int cluster_idx = 0;
			double distance;
			double minDist = 100000;

			for(int i = 0; i < n_centers; i++)
			{
				distance = getDist(x, y, center_x[i], center_y[i]);
				if(minDist > distance){
					minDist = distance;
					cluster_idx = i;
				}
			}
			
			one_key.set( cluster_idx );
			context.write( one_key, value );
		}

		protected void setup(Context context) throws IOException, InterruptedException
                {
                        Configuration conf = context.getConfiguration();
                        n_centers = conf.getInt("n_centers", -1);
                        center_x = new double[n_centers];
                        center_y = new double[n_centers];

                        for(int i = 0; i < n_centers; i++)
                        {
                                center_x[i] = conf.getFloat( "center_x_"+i , 0 );
                                center_y[i] = conf.getFloat( "center_y_"+i , 0 );
                        }
                }

	}

	public static class KMeansCombiner extends Reducer<IntWritable, Text, IntWritable, Text>
	{
		private Text outputValue = new Text();
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			double x_total = 0;
			double y_total = 0;
			int cnt = 0;
			for(Text val : values) {
				StringTokenizer itr = new StringTokenizer(val.toString(), ",");
				double x = Double.parseDouble(itr.nextToken().trim());
				double y = Double.parseDouble(itr.nextToken().trim());
				x_total += x;
				y_total += y;
				cnt++;
			}
			
			outputValue.set(x_total + "," + y_total + "," + cnt);
			context.write(key, outputValue);
		}
	}	

	public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text>
	{
		private Text result = new Text();
		public void reduce(IntWritable key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			for(Text val : values) {
				StringTokenizer itr = new StringTokenizer(val.toString(), ",");
				double x_total = Double.parseDouble(itr.nextToken().trim());
				double y_total = Double.parseDouble(itr.nextToken().trim());
				int cnt = Integer.parseInt(itr.nextToken().trim());
			
				double x = x_total / (double)cnt;
				double y = y_total / (double)cnt;

				result.set(x + "," + y);
				context.write(key, result);
			}
		}
	}


	public static void main(String[] args) throws Exception
        {
		int check = 0;
		int k = 2;
                Configuration conf = new Configuration();
		initKMeans(conf, k);
                for(int i = 0; i < 2; i++)
                {
                        Job job = new Job(conf, "KMeansCom");
                        job.setJarByClass(KMeans.class);
                        job.setMapperClass(KMeansMapper.class);
			job.setCombinerClass(KMeansCombiner.class);
                        job.setReducerClass(KMeansReducer.class);
                        job.setOutputKeyClass(IntWritable.class);
                        job.setOutputValueClass(Text.class);
                        job.setInputFormatClass(TextInputFormat.class);
                        FileInputFormat.addInputPath(job, new Path(args[0]));
                        FileOutputFormat.setOutputPath(job, new Path(args[1]));
                        FileSystem.get(job.getConfiguration()).delete(new Path(args[1]), true);
                        job.waitForCompletion(true);
			updateKMeans(conf, k);
		
			for(int j = 0; j < n_centers; j++) {
				double x = conf.getFloat("center_x_" + j, 0);
				double y = conf.getFloat("center_y_" + j, 0);
				if(center_x[j] != x || center_y[j] != y){
					check = 1;
				}
			}

			if(check == 0)	return;
                }
        }

	public static void initKMeans(Configuration conf, int k)
	{
		conf.setInt("n_centers", k);
		conf.setFloat("center_x_0", 0);
		conf.setFloat("center_y_0", 0);
		conf.setFloat("center_x_1", 7);
		conf.setFloat("center_y_1", 7);
		
		/*
		Random r = new Random();
		for(int i = 0; i < k; i++){
			conf.setFloat("center_x_" + i, r.nextInt()%10);
			conf.setFloat("center_y_" + i, r.nextInt()%10);
			System.out.println(conf.getFloat("center_x_" + i, 0));
			System.out.println(conf.getFloat("center_y_" + i, 0));
		}*/
	}	

        public static void updateKMeans(Configuration conf, int k) throws Exception {
                FileSystem dfs = FileSystem.get(conf);
                Path filenamePath = new Path("/output/part-r-00000");
                FSDataInputStream in = dfs.open(filenamePath);
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));

                String line = reader.readLine();
                while(line != null)
                {	
                        StringTokenizer itr = new StringTokenizer(new String(line));
			int cluster_id = Integer.parseInt(itr.nextToken().trim());
			String value = itr.nextToken().trim();
			
			StringTokenizer st = new StringTokenizer(value, ",");
                        double x = Double.parseDouble(st.nextToken().trim());
                        double y = Double.parseDouble(st.nextToken().trim());
                      	
			conf.setFloat("center_x_" + cluster_id,(float)x);
			conf.setFloat("center_y_" + cluster_id, (float)y);
			System.out.println("update: " + cluster_id + " " + x + " " + y);
                        line = reader.readLine();
                }
        }
}
                                
