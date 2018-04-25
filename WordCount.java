import java.io.*;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
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

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      String countryName = conf.get("countryName");
      String[] split = value.toString().split(",+");

      if (split[3].equals(countryName)) {
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

  public static int AirportByCountry(String inputPath, String outputPath, String countryName) throws Exception {
    Configuration conf = new Configuration();
    conf.set("countryName", countryName);
    Job job = Job.getInstance(conf, "Airport by country");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(AirportCountryMapper.class);
    job.setCombinerClass(AirportCountryReducer.class);
    job.setReducerClass(AirportCountryReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    return(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class AirlineStopMapper
       extends Mapper<Object, Text, Text, NullWritable>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      String sourcePort = conf.get("sourcePort");
      String destinationPort = conf.get("destinationPort");
      String stops = conf.get("stops");
      String[] split = (value.toString().replaceAll(",,", ", ,")).split(",+");

      if (split[3].equals(sourcePort) && split[6].equals(destinationPort) && 
          Integer.parseInt(split[10]) <= Integer.parseInt(stops)) {
        word.set(split[2] + " " + split[10]);
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

  public static int AirlineStops(String inputPath, String outputPath, String sourcePort, 
                                 String destinationPort, String stops) throws Exception {
    Configuration conf = new Configuration();
    conf.set("sourcePort", sourcePort);
    conf.set("destinationPort", destinationPort);
    conf.set("stops", stops);
    Job job = Job.getInstance(conf, "Airlines having stops");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(AirlineStopMapper.class);
    job.setCombinerClass(AirlineStopReducer.class);
    job.setReducerClass(AirlineStopReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    return(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class CodeShareMapper
       extends Mapper<Object, Text, Text, NullWritable>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] split = (value.toString().replaceAll(",,", ", ,")).split(",+");

      if (split[9].equals("Y") && !split[2].equals("\\N")) {
        word.set(split[2]);
        context.write(word, NullWritable.get());
      }
    }
  }

  public static class CodeShareReducer
       extends Reducer<Text,NullWritable,Text,NullWritable> {

    public void reduce(Text key, Iterable<NullWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      context.write(key, NullWritable.get());
    }
  }

  public static int CodeShare(String inputPath, String outputPath) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Code Share");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(CodeShareMapper.class);
    job.setCombinerClass(CodeShareReducer.class);
    job.setReducerClass(CodeShareReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    return(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class ActiveAirlinesMapper
       extends Mapper<Object, Text, Text, NullWritable>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      String countryName = conf.get("countryName");
      String[] split = (value.toString().replaceAll(",,", ", ,")).split(",+");

      if (split[6].equals(countryName) && split[7].equals("\"Y\"")) {
        word.set(split[1]);
        context.write(word, NullWritable.get());
      }
    }
  }

  public static class ActiveAirlinesReducer
       extends Reducer<Text,NullWritable,Text,NullWritable> {

    public void reduce(Text key, Iterable<NullWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      context.write(key, NullWritable.get());
    }
  }

  public static int ActiveAirlines(String inputPath, String outputPath, String countryName) throws Exception {
    Configuration conf = new Configuration();
    conf.set("countryName", countryName);
    Job job = Job.getInstance(conf, "Active Airlines");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(ActiveAirlinesMapper.class);
    job.setCombinerClass(ActiveAirlinesReducer.class);
    job.setReducerClass(ActiveAirlinesReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    return(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class AggregateCountryMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] split = value.toString().split(",+");

      word.set(split[3]);
      context.write(word, one);
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static int AggregateByCountry(String inputPath, String outputPath) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Airport by country");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(AggregateCountryMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    return(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class AirlineCityMapper
       extends Mapper<Object, Text, Text, NullWritable>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] split = value.toString().split(",+");


      if (!split[2].equals("\\N") && !split[5].equals("\\N") && !split[8].equals("\\N")) {
        word.set(split[5] + " " + split[2]);
        context.write(word, NullWritable.get());
        word.set(split[8] + " " + split[2]);
        context.write(word, NullWritable.get());
      }
    }
  }

  public static class NullReducer
       extends Reducer<Text,NullWritable,Text,NullWritable> {

    public void reduce(Text key, Iterable<NullWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      context.write(key, NullWritable.get());
    }
  }

  public static int AirlineCity(String inputPath, String outputPath) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Airlines in Cities");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(AirlineCityMapper.class);
    job.setCombinerClass(NullReducer.class);
    job.setReducerClass(NullReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    return(job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
  	AirportByCountry(args[0], args[3], "\"Japan\"");
    FileUtils.deleteDirectory(new File("output"));
    AirlineStops(args[2], args[3], "ACE", "BFS", "0");
    FileUtils.deleteDirectory(new File("output"));
    ActiveAirlines(args[1], args[3], "\"Japan\"");
    FileUtils.deleteDirectory(new File("output"));
    AggregateByCountry(args[0], args[3]);
    FileUtils.deleteDirectory(new File("output"));
    CodeShare(args[2], args[3]);
    FileUtils.deleteDirectory(new File("output"));
    AirlineCity(args[2], args[3]);
  }
}