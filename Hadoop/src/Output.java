import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class Output {
	
	public static class Map3 extends Mapper<LongWritable, Text, IntWritable, Text>{
		
		public void map(LongWritable key, Text line,Context context)throws IOException,InterruptedException
		{
			context.write(new IntWritable(1), line);
		}
		
	}
	
	public static class Reducer3 extends Reducer<IntWritable, Text, Text, Text>{
		
    public void reduce(IntWritable values,Iterable<Text> stock,Context context) throws IOException, InterruptedException
	{
    	ArrayList<String> values1 = new ArrayList<String>();
			
			for(Text t:stock){
				values1.add(t.toString());			
			}
			
			int lastindex = values1.size()-1;
			int lastindex2= values1.size()-10;
			context.write(new Text("Stocks with top 10 volatilities"), new Text(""));
			
			int i=0;
			while( i <=9)
			{
				context.write(new Text(values1.get(i)), new Text(""));
				i++;
			}
			
			context.write(new Text("Stocks with least 10 volatilities"),new Text("") );
			while( lastindex>=lastindex2)
			{
				context.write(new Text(values1.get(lastindex)), new Text(""));
				lastindex--;
			}
				
			
	}
	}
	
}
