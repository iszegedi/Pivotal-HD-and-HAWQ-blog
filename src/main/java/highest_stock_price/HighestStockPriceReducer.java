package highest_stock_price;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HighestStockPriceReducer extends
Reducer<Text, Text, NullWritable, Text> {
	Text key = new Text();
	Text val = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<Text> counts,
			Context context) throws IOException, InterruptedException {
		double maxadjclose = 0.0;
		
		for (Text c : counts) {
			String[] tokens = c.toString().split("\t");
			String date = tokens[0];
			double adjclose = Double.parseDouble(tokens[1]);
			if (adjclose > maxadjclose) {
				maxadjclose = adjclose;
				val.set(key.toString() + ":\t" + date + "\t" 
				+ String.valueOf(maxadjclose));
			}
		}
		
		context.write(NullWritable.get(), val);
	}
}
