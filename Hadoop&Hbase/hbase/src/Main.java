
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Main{


	public static void main(String[] args){
		Boolean status =false;
       System.out.print("starting....");
       Double ending = (double) new Date().getTime();
		Configuration conf = HBaseConfiguration.create();
		try {
			
			HBaseAdmin admin = new HBaseAdmin(conf);
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("table1"));
			tableDescriptor.addFamily(new HColumnDescriptor("volatility"));
			if ( admin.isTableAvailable("table1")){
				admin.disableTable("table1");
				admin.deleteTable("table1");
			}
			admin.createTable(tableDescriptor);
            System.out.println("table created/.......");
            Scan scan = new Scan();
			scan.setCaching(500);        
			scan.setCacheBlocks(false);  

			Job job_one = Job.getInstance();
			job_one.setJarByClass(Job1.class);
			FileInputFormat.addInputPath(job_one, new Path(args[0]));
			job_one.setInputFormatClass(TextInputFormat.class);
			job_one.setMapperClass(Job1.Map.class);
			job_one.setMapOutputKeyClass(Text.class);
			job_one.setMapOutputValueClass(Text.class);
			TableMapReduceUtil.initTableReducerJob(
					"table1",       
					Job1.Reducer1.class,    
					job_one);
			
			job_one.setNumReduceTasks(1);
			job_one.waitForCompletion(true);
			admin.close();
			
			System.out.println("job 1 completed........................................\n........\n....");
			
			HBaseAdmin admin2 = new HBaseAdmin(conf);
			/*HTableDescriptor tableDescriptor2 = new HTableDescriptor(TableName.valueOf("table2"));
			tableDescriptor2.addFamily(new HColumnDescriptor("newvalue"));
			//tableDescriptor2.addFamily(new HColumnDescriptor("price"));
			if ( admin2.isTableAvailable("table2")){
				admin2.disableTable("table2");
				admin2.deleteTable("table2");
			}
			admin2.createTable(tableDescriptor2);*/
			Scan scan2 = new Scan();
			scan2.setCaching(500);        
			scan2.setCacheBlocks(false);  
			Job job_two = Job.getInstance(conf);
			job_two.setJarByClass(Job2.class);
		
			TableMapReduceUtil.initTableMapperJob(
					"table1",       
					scan,               
					Job2.Map.class,     
					Text.class,
					Text.class,         
					job_two);               
					
			job_two.setMapperClass(Job2.Map.class);
			job_two.setReducerClass(Job2.Reducer2.class);
			job_two.setMapOutputKeyClass(Text.class);
			job_two.setMapOutputValueClass(Text.class);
			FileOutputFormat.setOutputPath(job_two,new Path("Output_"+args[0]));
			job_two.setNumReduceTasks(1);
			status=job_two.waitForCompletion(true);
			
			admin2.close();
			
			if(status==Boolean.TRUE)
			{
				Double beginning = (double) new Date().getTime();
				System.out.println(" Job Runtime(time of execution): " +( beginning-ending)/1000 +" seconds");
				
			}
			
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}

