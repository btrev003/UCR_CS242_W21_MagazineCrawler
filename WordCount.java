import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.StringReader;
import java.util.HashMap;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
	  // Iterate through entire json file line by line
	  BufferedReader bufReader = new BufferedReader(new StringReader(value.toString()));
	  String line=null;
	  while( (line=bufReader.readLine()) != null )
	  {
		// Extract image title
		int startIndex = line.indexOf("\"title\": \"") + 10; // Starting index of title
		int endIndex = line.indexOf("\", \"image_urls\""); // Ending index of title
		String title = line.substring(startIndex, endIndex);
		
		// Extract image ID
		startIndex = line.indexOf("\"uid\"") + 7; // Starting index of image ID
		endIndex = line.indexOf(", \"title\":"); // Ending index of iamge ID
		string imageID = line.substring(startIndex, endIndex);
		one.set(Integer.parseInt(imageID));
		
		// Emit word:imageID for every word in the title
		for(String term : title.split("\\s+")) {
			word.set(term);
			context.write(word, one);
		}
	  }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      // Stores all documents and the frequency that the word appears in
	  HashMap<Integer, Integer> output_dict = new HashMap<Integer, Integer>();
	  
	  for(IntWritable val : values) {
		// If image ID exists: Increment its frequency
		if(output_dict.containsKey(val.get())) {
			int currentFreq = output_dict.get(val.get());
			output_dict.put(val.get(), currentFreq + 1);
		}
		// Otherwise add it
		else {
			output_dict.put(val.get(), 1);
		}
	  }
	  String resultString;
	  for(Integer imgID : output_dict.keySet()) {
		resultString += String.valueOf(imgID) + ";" + output_dict.get(imgID);
	  }
      result.set(resultString);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "WordCount");
    //Job job = Job.getInstance(conf, "word count");
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