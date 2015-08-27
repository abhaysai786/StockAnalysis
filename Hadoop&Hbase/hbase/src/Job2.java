import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

public class Job2 {

	
		public static class Map extends TableMapper<Text, Text>{
			
			public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
	        	
				
				String nameofstock = new String(value.getRow());
				System.out.println(nameofstock);
	        	
	        	String complexkey = new String(value.getValue(Bytes.toBytes("volatility"), Bytes.toBytes("volatility")));
	          	System.out.println("key: 1, complexvalue: "+ complexkey);
	        	
	        	
	        	context.write(new Text("1"), new Text(complexkey));
	   	}
		}
			
		public static class Reducer2 extends Reducer<Text, Text, Text,	Text>{
			public void reduce(Text key, Iterable<Text> listvalues, Context context) throws IOException, InterruptedException{

	            ArrayList<String> values1 = new ArrayList<String>();
				for (Text t : listvalues) {
					values1.add(t.toString());
				}
				
				Collections.sort(values1,new Comparator<String>(){
					public int compare(String a1,String a2)
					{
					       String[] x =a1.split(" ");
					       String[] y =a2.split(" ");
					       Double x1 = Double.parseDouble(x[1]);
					       System.out.println("value of x1:"+x1);
					       Double x2 = Double.parseDouble(y[1]);
					       return x1.compareTo(x2);
					       
					}
				});
				
				int lastindex = values1.size() - 1;
				int lastindex2 = values1.size() - 10;
				
				context.write(new Text("Stocks with top 10 volatilities"),
						new Text(""));

				int i = 0;
				while (i <= 9) {
					context.write(new Text(values1.get(i)), new Text(""));
					i++;
				}

				context.write(new Text("Stocks with least 10 volatilities"),
						new Text(""));
				while (lastindex >= lastindex2) {
					context.write(new Text(values1.get(lastindex)), new Text(""));
					lastindex--;
				}

				
				
				
				
              
			}
		}
	}

