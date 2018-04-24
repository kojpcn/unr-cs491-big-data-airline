import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class AirportCountryMapper
       extends Mapper<Object, Text, Text, NullWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String argument1 = "ACE";
    private String argument2 = "BFS";
    private String argument3 = "0";

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] split = value.toString().split(",+");
      if (split[3].equals(argument1)) {
        word.set(split[1]);
        context.write(word, NullWritable.get());
      }
    }
  }

  public static class AirportCountryReducer
       extends Reducer<Text,NullWritable,Text,NullWritable> {

    public void reduce(Text key, Iterable<NullWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      context.write(key, NullWritable.get());
    }
  }

  public static class AirlineStopMapper
       extends Mapper<Object, Text, Text, NullWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String argument1 = "ACE";
    private String argument2 = "BFS";
    private String argument3 = "0";

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] split = (value.toString().replaceAll(",,", ", ,")).split(",+");
      System.out.println(split[2] + " " + split[4] + " " + split[7]);
      if (split[2].equals(argument1) && split[4].equals(argument2) && 
          Integer.parseInt(split[7]) <= Integer.parseInt(argument3)) {
        word.set(split[0] + " " + split[7]);
        context.write(word, NullWritable.get());
      }
    }
  }

  public static class AirlineStopReducer
       extends Reducer<Text,NullWritable,Text,NullWritable> {
    //private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<NullWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      // int sum = 0;
      // for (IntWritable val : values) {
      //   sum += val.get();
      // }
      // result.set(sum);
      context.write(key, NullWritable.get());
    }
  }

  public static int AirportByCountry(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Airport by country");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(AirportCountryMapper.class);
    job.setCombinerClass(AirportCountryReducer.class);
    job.setReducerClass(AirportCountryReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[3]));
    return(job.waitForCompletion(true) ? 0 : 1);
  }

  public static int AirlineStops(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Airlines having stops");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(AirlineStopMapper.class);
    job.setCombinerClass(AirlineStopReducer.class);
    job.setReducerClass(AirlineStopReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[2]));
    FileOutputFormat.setOutputPath(job, new Path(args[3]));
    return(job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
  	AirportByCountry(args);
    AirlineStops(args);
    String[]entries = index.list();
    for(String s: entries){
      File currentFile = new File(index.getPath(),s);
      currentFile.delete();
    }
  }
}