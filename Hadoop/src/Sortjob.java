import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;



public class Sortjob {

static int checker=0;





	public static class Map2 extends Mapper<LongWritable, Text, DoubleWritable, Text>
	{
		String args[];
		
		public void map (LongWritable key, Text line,Context context)throws IOException,InterruptedException
		{
			args= new String[2];
			String data = line.toString();
			args = data.split("\\s+");
		    String[] name = args[1].trim().split("\\.");
			context.write(new DoubleWritable(Double.parseDouble(args[0])), new Text(name[0]));
		}		
	}
	
	public static class Reducer2 extends Reducer<IntWritable, Text, Text, Text>{
		
		
		public void reduce(DoubleWritable values,Iterable<Text> stock,Context context) throws IOException, InterruptedException
		{
			
			
			Iterator<Text> s = stock.iterator();
			Text y = s.next();		
			String z = y.toString()+" "+values.toString();		
			context.write(new Text(z),new Text(""));
				
			}
			
	
			
			
			
			
		}
	}
	

