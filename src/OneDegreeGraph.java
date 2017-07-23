import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class OneDegreeGraph {
	public static class OneDegreeGraphMapper extends Mapper <Object,Text,Text,Text> {
		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {		
			String[] info = value.toString().split("\t");	
			ArrayList<String> output = new ArrayList<String>();
			String movie = info[0];
			String[] actors = info[1].split("\\|");
			for(String actor:actors)
			{
				output.clear();
				output.add(movie);
				for(String actor_1:actors)
				{
					if(actor_1.equals(actor))
						continue;
					output.add(actor_1);
				}
				context.write(new Text(actor), new Text(StringUtils.join(output,"|")));
			}
		}
	}
	public static class OneDegreeGraphReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {	 
	    	  
			ArrayList<String> info = new ArrayList<String>();			
			for (Text value : values){
				info.add(value.toString());
	    	}
			context.write(key, new Text(StringUtils.join(info,"\t")));
		}
	}
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
 
		Job job = Job.getInstance(conf,"OneDegreeGraph");
		job.setJarByClass(OneDegreeGraph.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(OneDegreeGraphMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(OneDegreeGraphReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	
		job.waitForCompletion(true);    
	}
}
