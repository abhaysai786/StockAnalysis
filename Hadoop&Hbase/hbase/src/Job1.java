import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
public class Job1 {
	/**
	 * @author ruhansa
	 * read files line by line, put the data into hbase style table
	 * input: <key, value>, key: line number, value: line
	 * output: <key, value>, key: rowid, value: hbase row content
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
		
	public static class Reducer1 extends TableReducer<Text,Text,ImmutableBytesWritable>{
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
				
				Put p = new Put(Bytes.toBytes(key.toString()));
				p.add(Bytes.toBytes("volatility"), Bytes.toBytes("volatility"), Bytes.toBytes(key.toString()+" "+volatility.toString()));
				context.write(null,p);
				
			}
			
			
		}
	}
}
