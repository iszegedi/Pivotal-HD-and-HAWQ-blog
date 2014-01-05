package highest_stock_price;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HighestStockPriceMapper extends
Mapper<LongWritable, Text, Text, Text>  {
	Text key = new Text();
	Text val = new Text();

	@Override
	protected void map(LongWritable offset, Text text, Context context)
			throws IOException, InterruptedException {
		String[] tokens = text.toString().split(",");
		String symbol = tokens[0];
		String date = tokens[1];
		String adjclose = tokens[7];
		key.set(symbol);
		val.set(date + "\t" + adjclose);
		context.write(key, val);
	}
}
