import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BFS {
	public final static int DEPTH = 6;
	public static void main(String[] args) throws Exception {		
		
		Configuration conf = new Configuration();
		String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (remainingArgs.length != 3) {
			System.err.println("Usage: BFS <data> <inverted> <cachePath>");
			System.exit(2);
		}
		String dataPath = remainingArgs[0]; 
		String invertedDataPath = remainingArgs[1]; 
		String cachePath = remainingArgs[2] + "openNode";
		for(int i = 1;i < DEPTH; ++i) {
	        Job invert = Job.getInstance(conf, "BFS");
	        invert.setJarByClass(BFS.class);
	
	        MultipleInputs.addInputPath(invert, new Path(dataPath), TextInputFormat.class,
	        		BFSMapper.class);
	        MultipleInputs.addInputPath(invert, new Path(invertedDataPath), TextInputFormat.class,
	        		BFSMapper.class);
	        if(i != 1) {
		        MultipleInputs.addInputPath(invert, new Path(cachePath + (i-1) + "/"), TextInputFormat.class,
		        		BFSMapper.class);
	        }
	        invert.setMapOutputKeyClass(Text.class);
	        
	        String outputPath = cachePath + i + "/";
	        Path path = new Path(outputPath);
	        FileSystem fileSystem = path.getFileSystem(conf);
	        if (fileSystem.exists(path)) {  
	            fileSystem.delete(path, true);
	        }  
	        
	        FileOutputFormat.setOutputPath(invert, new Path(outputPath));
	        invert.setOutputFormatClass(TextOutputFormat.class);
	        invert.setOutputKeyClass(Text.class);
	        invert.setOutputValueClass(Text.class);
	        invert.setReducerClass(BFSReducer.class);
	        System.exit(invert.waitForCompletion(true) ? 0 : 1);
		}
	}
	public static class BFSMapper extends Mapper<LongWritable, Text, Text, Text> {
	    @Override
	    public void map(LongWritable key, Text value, Context context)
	          throws IOException, InterruptedException {
      	  	  String[] info = value.toString().split("\t");
      	  	  if(info == null || info.length < 2) return;
    		  String actor = info[0];	
    		  String[] movies = info[1].split("\\|");
    		  for(String movie: movies) {
    			  context.write(new Text(movie), new Text(actor));
    		  }
	    }
	}
	public static class BFSReducer extends Reducer<Text,Text,Text,Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<String> actors = new ArrayList<String>();
			for(Text value: values) {
				actors.add(value.toString());
			}
			if(actors.size() == 1) return;
			context.write(key, new Text(StringUtils.join(actors.toArray(),"|")));
		}
	}
}
