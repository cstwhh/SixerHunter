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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedData {
	public static void main(String[] args) throws Exception {		
		
		Configuration conf = new Configuration();
		String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (remainingArgs.length != 2) {
			System.err.println("Usage: InvertedData <in> <out>");
			System.exit(2);
		}
		String inputPath = remainingArgs[0]; String outputPath = remainingArgs[1];

        Job invert = Job.getInstance(conf, "InvertedData");
        invert.setJarByClass(InvertedData.class);

        // Input / Mapper
        FileInputFormat.addInputPath(invert, new Path(inputPath));
        invert.setMapOutputKeyClass(Text.class);
        invert.setMapperClass(InvertedDataMapper.class);

        // Output / Reducer
        // 判断output文件夹是否存在，如果存在则删除  
        Path path = new Path(outputPath);
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(path)) {  
            fileSystem.delete(path, true);
        }  
        FileOutputFormat.setOutputPath(invert, new Path(outputPath));
        invert.setOutputFormatClass(TextOutputFormat.class);
        invert.setOutputKeyClass(Text.class);
        invert.setOutputValueClass(Text.class);
        invert.setReducerClass(InvertedDataReducer.class);

        System.exit(invert.waitForCompletion(true) ? 0 : 1);
	}
	public static class InvertedDataMapper extends Mapper<LongWritable, Text, Text, Text> {
	    @Override
	    public void map(LongWritable key, Text value, Context context)
	          throws IOException, InterruptedException {
      	  	  String[] info = value.toString().split("\t");
      	  	  if(info == null || info.length < 2) return;
    		  String actor = info[0];	
    		  String[] movies = info[1].split(",");
    		  for(String movie: movies) {
    			  context.write(new Text(movie), new Text(actor));
    		  }
	    }
	}
	public static class InvertedDataReducer extends Reducer<Text,Text,Text,Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<String> actors = new ArrayList<String>();
			for(Text value: values) {
				actors.add(value.toString());
			}
			context.write(key, new Text(StringUtils.join(actors.toArray(),",")));
		}
	}
}
