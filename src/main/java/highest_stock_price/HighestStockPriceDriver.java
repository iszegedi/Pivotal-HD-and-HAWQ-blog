package highest_stock_price;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import highest_stock_price.HighestStockPriceMapper;
import highest_stock_price.HighestStockPriceReducer;;

public class HighestStockPriceDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		job.setJarByClass(HighestStockPriceDriver.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		outputPath.getFileSystem(job.getConfiguration()).delete(outputPath,
				true);

		job.setMapperClass(HighestStockPriceMapper.class);
		job.setReducerClass(HighestStockPriceReducer.class);

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(1);
		job.waitForCompletion(true);
		return 0;

	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: " + "HighestStockPriceDriver"
					+ "<in> <out>");
			System.exit(2);
		}
		int exitCode = ToolRunner.run(new HighestStockPriceDriver(),
				args);
		System.exit(exitCode);

	}

}
