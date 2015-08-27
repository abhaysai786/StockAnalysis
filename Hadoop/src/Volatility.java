
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Volatility {
	
	/**
	 * @author hduser
	 * seperate the files into lines and lines into token
	 * input: <key, value>, key: line number, value: line
	 * output: <key, value>, key: each word, value: number of occurence 
	 * e.g. input <line1, hello a bye a>
	 * 		output<hello, 1>,<a, 1>, <bye, 1>,<a, 1>
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private final static Text file = new Text(); // value = 1
		private Text word = new Text(); // output key
		
		String[] valuesinline = new String[7];
		public void map(LongWritable key, Text value, Context context){
			String line = value.toString();
			
			if (!line.contains("Date")) {

				valuesinline = line.trim().split(",");
				
				String fileName = ((FileSplit) context.getInputSplit()).getPath().getName().toString();
				//FileSplit fsp = (FileSplit) Rep.getInputSplit();
				//String file = fsp.getPath().getName();
         
				// System.out.println(file+"keytosend");
				String valuetosend = valuesinline[0] + "@" + valuesinline[6];
				//System.out.println(valuetosend);

				//context.write(new Text(fileName), new Text(valuetosend));
                file.set(fileName);
			    word.set(valuetosend);
				try {
					context.write(file, word);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
			}
			
		}
	}
	
	/**
	 * @author hduser
	 * Count the number of times the given keys occur
	 * input: <key, value>, key = Text, value: number of occurrence
	 * output: <key, value>, key = Text, value = number of occurrence
	 * 
	 * */
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> listvalues, Context context) throws IOException, InterruptedException{

			
			
			
			String prevlinmonth = null;
			Double prevlincloseprice = 0.0;

			Double bcloseprice = 0.0;
			Double ecloseprice = 0.0;

			Double monthlyrateofreturn = 0.0;
			ArrayList<Double> monthlyratereturn = new ArrayList<Double>();

			String[] values = new String[2];
			//String[] date = new String[3];
			int flag = 0;
			

			Double xbar = 0.0;

			Double volatility = 0.0;

			// System.out.println(arg0.toString() + "keytosend");
            
			String str =null;
				for(Text t:listvalues)
				{
					System.out.println(t.toString());
				  str= t.toString();
				//  System.out.println(str +"strhereiam");
				  
				   values = str.split("@");
				  // System.out.println(values[0]+"values[0]");
				   String[] date = values[0].split("-");
				 
				
				//   System.out.println(date[1]+"date[1] \n");
				  // System.out.println(date[2]+"date[2]");
				
				   if (flag == 0) {
					prevlinmonth = date[1];
					// System.out.println(values[6]);
					bcloseprice = Double.parseDouble(values[1]);
					flag = 1;
				    }
				   
			    	if (!prevlinmonth.equals(date[1])) {
					ecloseprice = prevlincloseprice;
					monthlyrateofreturn = (ecloseprice - bcloseprice) / bcloseprice;
					monthlyratereturn.add(monthlyrateofreturn);
					bcloseprice = Double.parseDouble(values[1]);
				}

				prevlinmonth = date[1];
				prevlincloseprice = Double.parseDouble(values[1]);
				
				}
				
				
			ecloseprice = prevlincloseprice;
			monthlyrateofreturn = (ecloseprice - bcloseprice) / bcloseprice;
			monthlyratereturn.add(monthlyrateofreturn);
							
			Integer counter = 0;
			for (Double e : monthlyratereturn) {
				xbar = xbar + e;
				counter++;
			}

			xbar = xbar / counter;

			
			Double vol = 0.0;
			for (Double d : monthlyratereturn) {
				vol += (d - xbar) * (d - xbar);

			}

			volatility = Math.sqrt(vol / (counter - 1));

			if ((!volatility.isNaN()) && (volatility !=0))
			{	
				context.write(new Text(volatility.toString()),new Text(key));
			}
		}
	}
	
	public static void main(String[] args) {
	
		try {
			// Create a new Job
		     Job job = Job.getInstance();
		     job.setJarByClass(Volatility.class);
		     Job job2 = Job.getInstance();
		     job2.setJarByClass(Sortjob.class);
		     Job job3 = Job.getInstance();
		     job3.setJarByClass(Output.class);
		     
			
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(1);
			
			job2.setMapperClass(Sortjob.Map2.class);
			job2.setReducerClass(Sortjob.Reducer2.class);
			job2.setMapOutputKeyClass(DoubleWritable.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setNumReduceTasks(1);
			
			job3.setMapperClass(Output.Map3.class);
			job3.setReducerClass(Output.Reducer3.class);
			job3.setMapOutputKeyClass(IntWritable.class);
			job3.setMapOutputValueClass(Text.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			job3.setNumReduceTasks(1);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			job3.setInputFormatClass(TextInputFormat.class);
			job3.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]+"_Intermediate1"));
			FileInputFormat.addInputPath(job2, new Path(args[1]+"_Intermediate1"));
			FileOutputFormat.setOutputPath(job2, new Path(args[1]+"_Intermediate2"));
			FileInputFormat.addInputPath(job3, new Path(args[1]+"_Intermediate2"));
			FileOutputFormat.setOutputPath(job3, new Path(args[1]));
			
			
		
			//job.setJarByClass(WordCount.class);
			job.waitForCompletion(true);
			//job2.setJarByClass(Sortjob.class);
			job2.waitForCompletion(true);
			job3.waitForCompletion(true);
			
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
