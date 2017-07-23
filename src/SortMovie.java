import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SortMovie {
	public static class SortMovieMapper extends Mapper <Object,Text,IntWritable,Text> {
		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {		
			String[] info = value.toString().split("\t");			
			String[] actors = info[1].split("|");
			int actorNum = -1*actors.length;
			context.write(new IntWritable(actorNum), value);	
		}
	}
	public static class SortMovieReducer extends Reducer<IntWritable, Text, Text, Text> {
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {	 
	    	  for (Text value : values){
	    		  context.write(value, new Text(""));
	    	  } 
		}
	}
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
 
		Job job = Job.getInstance(conf,"SortMovie");
		job.setJarByClass(SortMovie.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(SortMovieMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(SortMovieReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	
		job.waitForCompletion(true);
	    
	}
}
