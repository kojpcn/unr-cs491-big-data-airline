import java.io.*;
import java.util.*;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Scanner;
import java.util.ArrayList;
import java.nio.ByteBuffer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

	public static class AirportCountryMapper extends Mapper<Object, Text, Text, NullWritable>{

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String countryName = conf.get("countryName");
			String[] split = value.toString().split(",+");

			if (split[3].equals(countryName)) {
				word.set(split[1]);
				context.write(word, NullWritable.get());
			}
		}
	}

	public static class AirportCountryReducer extends Reducer<Text,NullWritable,Text,NullWritable> {

		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
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

	public static class AirlineStopMapper extends Mapper<Object, Text, Text, NullWritable>{

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
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

	public static class AirlineStopReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
		// private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			// int sum = 0;
			// for (IntWritable val : values) {
			//	 sum += val.get();
			// }
			// result.set(sum);
			context.write(key, NullWritable.get());
		}
	}

	public static int AirlineStops(String inputPath, String outputPath, String sourcePort, String destinationPort, String stops) throws Exception {
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

	public static class CodeShareMapper extends Mapper<Object, Text, Text, NullWritable>{

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = (value.toString().replaceAll(",,", ", ,")).split(",+");

			if (split[9].equals("Y") && !split[2].equals("\\N")) {
				word.set(split[2]);
				context.write(word, NullWritable.get());
			}
		}
	}

	public static class CodeShareReducer extends Reducer<Text,NullWritable,Text,NullWritable> {

		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
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

	public static class ActiveAirlinesMapper extends Mapper<Object, Text, Text, NullWritable>{

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String countryName = conf.get("countryName");
			String[] split = (value.toString().replaceAll(",,", ", ,")).split(",+");

			if (split[6].equals(countryName) && split[7].equals("\"Y\"")) {
				word.set(split[1]);
				context.write(word, NullWritable.get());
			}
		}
	}

	public static class ActiveAirlinesReducer extends Reducer<Text,NullWritable,Text,NullWritable> {

		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
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

	public static class AggregateCountryMapper extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split(",+");

			word.set(split[3]);
			context.write(word, one);
		}
	}

	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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

	public static class AirlineCityMapper extends Mapper<Object, Text, Text, NullWritable>{

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split(",+");


			if (!split[2].equals("\\N") && !split[5].equals("\\N") && !split[8].equals("\\N")) {
				word.set(split[5] + "," + split[2]);
				context.write(word, NullWritable.get());
				word.set(split[8] + "," + split[2]);
				context.write(word, NullWritable.get());
			}
		}
	}

	public static class NullReducer extends Reducer<Text,NullWritable,Text,NullWritable> {

		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
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

	public static class AggregateAirlineMapper extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split(",+");

			word.set(split[0]);
			context.write(word, one);
		}
	}

	public static int AirlineCityCount(String inputPath, String outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Count Airlines in Cities");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(AggregateAirlineMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class IntComparator extends WritableComparator {

    	public IntComparator() {
        	super(IntWritable.class);
    	}

    	@Override
    	public int compare(byte[] b1, int s1, int l1,
             			   byte[] b2, int s2, int l2) {

        	Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
        	Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

        	return v1.compareTo(v2) * (-1);
    	}
	}

  	public static class GreatestValMapper
       	   	    extends Mapper<Object, Text, IntWritable, Text>{

    	private Text word = new Text();

    	public void map(Object key, Text value, Context context
        	            ) throws IOException, InterruptedException {
      		String[] split = value.toString().split("\t+");

      		word.set(split[0]);
      		int valuePart = Integer.parseInt(split[1]);
      		context.write(new IntWritable(valuePart), word);
    	}
  	}

  	public static class GreatestValReducer
       	        extends Reducer<IntWritable,Text,IntWritable,Text> {

    	public void reduce(IntWritable key, Iterable<Text> values,
        	               Context context
            	           ) throws IOException, InterruptedException {
      		for (Text val : values) {
        		context.write(key, val);
      		}
    	}
  	}

  	public static int OrderGreatestVal(String inputPath, String outputPath) throws Exception {
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "Count Airlines in Cities");
    	job.setJarByClass(WordCount.class);
	    job.setMapperClass(GreatestValMapper.class);
	    job.setCombinerClass(GreatestValReducer.class);
	    job.setReducerClass(GreatestValReducer.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setSortComparatorClass(IntComparator.class);
	    FileInputFormat.addInputPath(job, new Path(inputPath));
	    job.setInputFormatClass(TextInputFormat.class);
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    job.setOutputFormatClass(TextOutputFormat.class);
	    return(job.waitForCompletion(true) ? 0 : 1);
 	}


	public static class CitytoCityMapper extends Mapper<Object, Text, Text, NullWritable>{

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String cityList = conf.get("cityList");
			String[] citySplit = cityList.split(",+");
			List<String> cityList2 = Arrays.asList(citySplit);
			String city2 = conf.get("city2");
			String visitedList = conf.get("visitedList");
			String[] visitedSplit = visitedList.split(",+");
			List<String> visitedList2 = Arrays.asList(visitedSplit);
			String[] split = value.toString().split(",+");

			if (cityList2.contains(split[5]) && !visitedList2.contains(split[8])) {
				if(split[8].equals(city2)) {
					word.set("!," + value.toString());
					context.write(word, NullWritable.get());
				}
				else {
					word.set(value.toString());
					context.write(word, NullWritable.get());
				}
			}
		}
	}

	public static int CitytoCity(String inputPath, String outputPath, String cityList, String city2, String visitedList) throws Exception {
		Configuration conf = new Configuration();
		conf.set("cityList", cityList);
		conf.set("city2", city2);
		conf.set("visitedList", visitedList);
		Job job = Job.getInstance(conf, "City to City");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(CitytoCityMapper.class);
		job.setCombinerClass(NullReducer.class);
		job.setReducerClass(NullReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class UniqueCityMapper extends Mapper<Object, Text, Text, NullWritable>{

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split(",+");

            if(!split[8].equals("\\N")) {
				word.set(split[8]);
				context.write(word, NullWritable.get());
			}
		}
	}

	public static int UniqueCities(String inputPath, String outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "UniqueCities");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(UniqueCityMapper.class);
		job.setCombinerClass(NullReducer.class);
		job.setReducerClass(NullReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return(job.waitForCompletion(true) ? 0 : 1);
	}

	public static List<String> findPath(String[] args, String city1, String city2) throws Exception {
		String line = null;
		String cityList = city1;
		String visitedList = "\\N";
		int stopCount = 0;

        while(true) {
			CitytoCity(args[2], ("output/stop" + Integer.toString(stopCount)), 
				                 cityList, city2, visitedList);

			try {
	            BufferedReader bufferedReader = new BufferedReader(
	            	             new FileReader("output/stop" + 
	            	             Integer.toString(stopCount) + "/part-r-00000"));
		        if((line = bufferedReader.readLine()) != null) {
			        String[] split = line.split(",+");
			        if(split[0].equals("!"))
			        	break;
			    }
			    bufferedReader.close();
			}
			catch(FileNotFoundException ex) {
	            System.out.println("Unable to open file");                
	        }
	        catch(IOException ex) {
	            System.out.println("Error reading file");                  
	        }

			UniqueCities(("output/stop" + Integer.toString(stopCount) + "/part-r-00000"), 
				         ("output/stop" + Integer.toString(stopCount) + "/cities"));

			try {
	            BufferedReader bufferedReader = new BufferedReader(
	            	             new FileReader("output/stop" + 
	            	             Integer.toString(stopCount) + "/cities/part-r-00000"));
	        	if((line = bufferedReader.readLine()) != null) {
	        		visitedList = (visitedList + "," + cityList);
			        cityList = line;
			        while((line = bufferedReader.readLine()) != null) {
			        	cityList = (cityList + "," + line);
			        }
			    }
			    else {
			    	return(new ArrayList<String>());
			    }
			    bufferedReader.close();
			}
			catch(FileNotFoundException ex) {
	            System.out.println("Unable to open file");                
	        }
	        catch(IOException ex) {
	            System.out.println("Error reading file");                  
	        }
	        stopCount++;
    	}

    	List<String> routeList = new ArrayList<String>();
    	String currentCity = null;

    	while(stopCount >= 0) {
			try {
	            BufferedReader bufferedReader = new BufferedReader(
	            	             new FileReader("output/stop" + 
	            	             Integer.toString(stopCount) + "/part-r-00000"));
		        if((line = bufferedReader.readLine()) != null) {
			        String[] split = line.split(",+");
			        if(split[0].equals("!")) {
			        	routeList.add(0, line.substring(2));
			        	currentCity = split[6];
			        }
			        else {
			        	do {
							String[] split2 = line.split(",+");
							if(split2[8].equals(currentCity)) {
					        	routeList.add(0, line);
					        	currentCity = split2[5];
					        	break;
							}
			        	} while((line = bufferedReader.readLine()) != null);
			        }
			    }
			    bufferedReader.close();
			}
			catch(FileNotFoundException ex) {
	            System.out.println("Unable to open file");                
	        }
	        catch(IOException ex) {
	            System.out.println("Error reading file");                  
	        }
	        stopCount--;
    	} 

        return routeList;
	}	

	public static void main(String[] args) throws Exception {
		Scanner scanner = new Scanner(System.in);
		System.out.printf("1. Airport and airline search engine\n2. Airline aggregation\n3. Trip recommendation"
			+ "\nYour choice? ");
		int choice = scanner.nextInt();
		if (choice == 1) {
			System.out.printf("1. List of airports in country X\n2. List of airlines with X stops\n" + 
				"3. List of airlines operating with code Share\n4. List of active airlines in the United States\n"
				+ "Your choice? ");
			choice = scanner.nextInt();
			scanner.nextLine();	// Remove nextline character from previous input
			if (choice == 1) {
				System.out.printf("Which country? ");
				String whichCountry = scanner.nextLine();
				AirportByCountry(args[0], args[3], "\"" + whichCountry + "\"");
			} else if (choice == 2) {
				System.out.printf("Which airport to start from? ");
				String sourcePort = scanner.nextLine();
				System.out.printf("Where is the destination? ");
				String destinationPort = scanner.nextLine();
				System.out.printf("How many stops? ");
				choice = scanner.nextInt();
				AirlineStops(args[2], args[3], sourcePort, destinationPort, Integer.toString(choice));
			} else if (choice == 3) {
				CodeShare(args[2], args[3]);
			} else if (choice == 4) {
				ActiveAirlines(args[1], args[3], "\"United States\"");
			} else {
				System.out.println("Invalid input, quitting...");
			}

		} else if (choice == 2) {
			System.out.printf("1. Country with most airports\n2. Cities with most traffic\nYour choice? ");
			choice = scanner.nextInt();
			scanner.nextLine();	// Remove nextline character from previous input
			if (choice == 1){
				AggregateByCountry(args[0], args[3]);
				OrderGreatestVal("output/part-r-00000", "output/output");
			} else if (choice == 2) {
				AirlineCity(args[2], args[3]);
				AirlineCityCount("output/part-r-00000", "output/output");
				OrderGreatestVal("output/output/part-r-00000", "output/output2");
			} else {
				System.out.println("Invalid input, quitting...");
			}
		} else if (choice == 3){
			System.out.printf("1. Check reachability\n2. Constrained reachability\n3. Bounded reachability\nYour choice? ");
			choice = scanner.nextInt();
			scanner.nextLine();	// Remove nextline character from previous input
			if (choice == 1){
				System.out.printf("Which airport to start from? ");
				String sourcePort = scanner.nextLine();
				System.out.printf("Where is the destination? ");
				String destinationPort = scanner.nextLine();
				List<String> isReachable = findPath(args, "\"" + sourcePort + "\"", "\"" + destinationPort + "\"");
				if (isReachable.isEmpty()) {
					System.out.println(destinationPort + " is not reachable from " + sourcePort);
				} else {
					System.out.println(destinationPort + " is reachable from " + sourcePort);
					for (String s : isReachable){
						System.out.println(s);
					}
				}
			} else if (choice == 2) {
				System.out.printf("Which airport to start from? ");
				String sourcePort = scanner.nextLine();
				System.out.printf("Where is the destination? ");
				String destinationPort = scanner.nextLine();
				System.out.printf("How many stops maximum? ");
				int numStops = scanner.nextInt();
				scanner.nextLine();	// Remove nextline character from previous input
				List<String> isReachable = findPath(args, "\"" + sourcePort + "\"", "\"" + destinationPort + "\"");
				if (isReachable.isEmpty()) {
					System.out.println(destinationPort + " is not reachable from " + sourcePort);
				} else if (isReachable.size() > numStops + 1) {
					System.out.println(destinationPort + " is not reachable from " + sourcePort + " with less than " + numStops + " stops.");
				} else {
					System.out.println(destinationPort + " is reachable from " + sourcePort + " in " + (isReachable.size() - 1) + " stops.");
					for (String s : isReachable){
						System.out.println(s);
					}
				}
			} else if (choice == 3) {
				//
			} else if (choice == 4) {
				//
			} else {
				System.out.println("Invalid input, quitting...");
			}
		} else{
			System.out.println("Invalid input, quitting...");
		}
	}
}