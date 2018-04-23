import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount{

	public static class TokenizerMapper extends Mapper<IntWritable, Text, Text, String>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context
						) throws IOException, InterruptedException{
			String[] split = value.toString().split(",+");
			if (split.length > 4){
				word.set(split[5]);
				try{
					context.write(word, split[0]);
				} catch (NumberFormatException e){
					// Cannot parse - ignore
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text,String,Text,String>{
		private String result = new String();

		public void reduce(Text key, Iterable<String> values, Context context) throws IOException, InterruptedException{
			// int sum = 0;
			for (String val : values){
				// sum += val.get();
				result = val;
			}
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}