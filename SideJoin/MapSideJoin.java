import java.io.*;
import java.util.*;
import java.net.URI;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.DistributedCache;

public class MapSideJoin
{
	public static class MapSideJoinMapper extends Mapper<Object, Text, Text, Text>{                 
		Hashtable<String,String> joinMap = new Hashtable<String,String>(); 
		
 		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{  
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			String ID = itr.nextToken();
			String price = itr.nextToken();
			String joinKey = itr.nextToken();
		
			Text outputKey = new Text(ID);
			Text outputValue = new Text();
			outputValue.set(price + " " + joinMap.get(joinKey));
			context.write(outputKey, outputValue);

 		}

		protected void setup(Context context) throws IOException, InterruptedException{  
 			Path[] cacheFiles = DistributedCache.getLocalCacheFiles( context.getConfiguration() );   
			BufferedReader br = new BufferedReader( new FileReader( cacheFiles[0].toString() ) );                         
			String line = br.readLine();  
			while( line != null ) {
                                 StringTokenizer itr = new StringTokenizer(line, "|");
                                 String category = itr.nextToken();
                                 String category_name = itr.nextToken();      
                           	 joinMap.put(category, category_name);
				 line = br.readLine();  
			}
		}
	}


	public static void main(String[] args) throws Exception {  
		Configuration conf = new Configuration();  
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
		if (otherArgs.length != 2) {   
			System.err.println("Usage: MapSideJoin <in> <out>");   
			System.exit(2);  
		}                   

		Job job = new Job(conf, "MapSideJoin");   
		DistributedCache.addCacheFile( new URI("/join_data/fileB.txt"), job.getConfiguration());  
		job.setJarByClass(MapSideJoin.class);
		job.setMapperClass(MapSideJoinMapper.class);
		job.setNumReduceTasks(0);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);
                System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

