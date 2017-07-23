import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
// 计算演员的rank分数
public class CalculateRank {
	public static void main(String[] args) throws Exception {		
		Configuration conf = new Configuration();
		String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (remainingArgs.length != 3) {
			System.err.println("Usage: CalculateRank <rankdata> <moviedata> <out>");
			System.exit(2);
		}
		String rankDataPath = remainingArgs[0]; 
		String movieDataPath = remainingArgs[1]; 
		String outputPath = remainingArgs[2];
		
        Job job = Job.getInstance(conf, "CalculateRank");
        job.setJarByClass(CalculateRank.class);
        MultipleInputs.addInputPath(job, new Path(rankDataPath), TextInputFormat.class,
        		RankDataMapper.class);
        MultipleInputs.addInputPath(job, new Path(movieDataPath), TextInputFormat.class,
        		MovieDataMapper.class);
        job.setMapOutputKeyClass(Text.class);
        // Output / Reducer
        // 判断output文件夹是否存在，如果存在则删除  
        Path path = new Path(outputPath);
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(path)) {  
            fileSystem.delete(path, true);
        }  
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(RankReducer.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	public static class RankDataMapper extends Mapper<LongWritable, Text, Text, Text> {
	    private static final Pattern rankPattern = Pattern.compile("(.*?)  (<\\d*?>)");
	    @Override
	    public void map(LongWritable key, Text value, Context context)
	          throws IOException, InterruptedException {
    	  	  String[] info = value.toString().split("\t");
    	  	  if(info == null || info.length < 2) return;
	  		  String actor = info[0];	
	  		  String[] movies = info[1].split("\\|");
	  		  for(String movie: movies) {
	  	          Matcher rankMatcher = rankPattern.matcher(movie);
	  		      if (rankMatcher.find()) {
//	  		         String name = rankMatcher.group(1);
//	  		         String rank = rankMatcher.group(2);
//	  		         System.out.println("FIND, movie: " + movie + ", name: " + name + ", rank: " + rank);
	  		         context.write(new Text(actor), new Text(movie));
	  		      } else {
//	   		         System.out.println("NOT FIND, movie: " + movie);
	  		      }
	  		  }
	    }
	}
	public static class MovieDataMapper extends Mapper<LongWritable, Text, Text, Text> {
	    @Override
	    public void map(LongWritable key, Text value, Context context)
	          throws IOException, InterruptedException {
    	  	  String[] info = value.toString().split("\t");
    	  	  if(info == null || info.length < 2) return;
	  		  String movie = info[0];
	  		  String[] actors = info[1].split("\\|");
	  		  int actorsNumber = actors.length;
	  		  for(String actor: actors) {
	  			  context.write(new Text(actor), new Text(movie + "|" + actorsNumber));
	  		  }
	    }
	}
	public static class RankReducer extends Reducer<Text, Text, Text, Text> {
	    private static final Pattern rankPattern = Pattern.compile("(.*?)  <(\\d*?)>");
	    private static final Pattern numberPattern = Pattern.compile("(.*?)\\|(\\d*)");
	    @Override
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    	String actor = key.toString();
	    	HashMap<String, Integer> ranks = new HashMap<String, Integer>();
	    	HashMap<String, Integer> numbers = new HashMap<String, Integer>();
	    	for(Text value: values) {
//	    		System.out.println("Reduce Get [" + key.toString() + "]:" + value.toString());
	    		String movie = value.toString();

	  	        Matcher rankMatcher = rankPattern.matcher(movie);
	  		    if (rankMatcher.find()) {
	  		       String name = rankMatcher.group(1);
	  		       String rank = rankMatcher.group(2);
//	  		         System.out.println("FIND, name: " + name + ", rank: " + rank);
	  		       ranks.put(name, Integer.valueOf(rank));
	  		    }
	  		    
	  	        Matcher numberMatcher = numberPattern.matcher(movie);
	  		    if (numberMatcher.find()) {
	  		       String name = numberMatcher.group(1);
	  		       String number = numberMatcher.group(2);
//	  		       System.out.println("FIND, name: " + name + ", number: " + number);
	  		       numbers.put(name, Integer.valueOf(number));
	  		    }
	    	}
	    	
  		    double score = 0.0;
			for (Entry<String, Integer> entry: numbers.entrySet()) {
			    String name = entry.getKey();
			    Integer rank = ranks.get(name);
			    if(rank == null) {
			    	// 没有rank信息，这里简单处理为直接跳过
			    	score += 0.5;
			    	continue;
			    }
			    else {
			    	int number = entry.getValue();
			    	if(rank > number) {	
			    		rank = number;
			    	}
//		  		    System.out.println("actor: " + actor + ", movie: " + name + ", rank: " + rank + ", number: " + number);
			    	score += ((1.0 + number - rank) / number);
			    }
			}
	    	if(score == 0.0) return;
//	    	context.write(new Text(actor), new Text(Integer.toString(numbers.entrySet().size())));
	    	context.write(new Text(actor), new Text(Double.toString(score)));
	    }
	}

}
