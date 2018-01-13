import java.io.*;
import java.util.*;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class TopK
{
	public static class EmpComparator implements Comparator<Emp> {
		public int compare(Emp x, Emp y) {
			if ( x.salary > y.salary ) return 1;
			if ( x.salary < y.salary ) return -1;
			return 0;
		}
	}

	public static void insertEmp(PriorityQueue q, int id, int salary, String dept_id, String emp_info, int topK) {
		Emp emp_head = (Emp) q.peek();

		if(q.size() < topK || emp_head.salary < salary){
			Emp emp = new Emp(id, salary, dept_id, emp_info);
			q.add(emp);
			if(q.size() > topK)	
				q.remove();
		}
	}

	public static class TopKMapper extends Mapper<Object, Text, Text, NullWritable> {
		 private PriorityQueue<Emp> queue ;
		 private Comparator<Emp> comp = new EmpComparator();
		 private int topK;

		 public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");

			if(itr.countTokens() < 4) return;
			int emp_id = Integer.parseInt(itr.nextToken().trim());
			String dept_id = itr.nextToken().trim();
			int salary = Integer.parseInt(itr.nextToken().trim());
			String emp_info = itr.nextToken().trim();
			insertEmp(queue, emp_id, salary, dept_id, emp_info, topK);
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Emp>( topK , comp);
		}

		 protected void cleanup(Context context) throws IOException, InterruptedException {
			while( queue.size() != 0 ) {
				Emp emp = (Emp) queue.remove();
				context.write( new Text( emp.getString() ), NullWritable.get() );
			}
		}
	}


	public static class TopKReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		private PriorityQueue<Emp> queue ;
		private Comparator<Emp> comp = new EmpComparator();
		private int topK;

		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(key.toString(), "|");
			if(itr.countTokens() < 4) return;
                        int emp_id = Integer.parseInt(itr.nextToken().trim());
                        String dept_id = itr.nextToken().trim();
                        int salary = Integer.parseInt(itr.nextToken().trim());
                        String emp_info = itr.nextToken().trim();
			insertEmp(queue, emp_id, salary, dept_id, emp_info, topK);
		}

		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Emp>( topK , comp);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			while( queue.size() != 0 ) {
				Emp emp = (Emp) queue.remove();
				context.write( new Text( emp.getString() ), NullWritable.get() );
			}
		}
	}

		public static void main(String[] args) throws Exception{
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			int topK = 3;
			conf.setInt("topK", topK);
			Job job = new Job(conf, "TopK");
			job.setJarByClass(TopK.class);
			job.setMapperClass(TopKMapper.class);
			job.setReducerClass(TopKReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}

class Emp {
       public int id;
       public int salary;
       public String dept_id;
       public String emp_info;

       public Emp(int id, int salary, String dept_id, String emp_info) {
             this.id = id;
             this.salary = salary;
             this.dept_id = dept_id;
             this.emp_info = emp_info;
       }

       public String getString(){
             return id + " | " + dept_id + " | " + salary + " | " + emp_info;
       }
}

