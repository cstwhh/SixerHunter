import java.io.IOException;
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

public class CalculateDegree {
	public static void main(String[] args) throws Exception {		
		
		Configuration conf = new Configuration();
		String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (remainingArgs.length != 2) {
			System.err.println("Usage: CalculateDegree <in> <out>");
			System.exit(2);
		}
		String inputPath = remainingArgs[0]; String outputPath = remainingArgs[1];

        Job calculate = Job.getInstance(conf, "CalculateDegree");
        calculate.setJarByClass(CalculateDegree.class);

        // Input / Mapper
        FileInputFormat.addInputPath(calculate, new Path(inputPath));
        calculate.setMapOutputKeyClass(LongWritable.class);
        calculate.setMapperClass(CalculateDegreeMapper.class);

        // Output / Reducer
        // 判断output文件夹是否存在，如果存在则删除  
        Path path = new Path(outputPath);
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(path)) {  
            fileSystem.delete(path, true);
        }  
        FileOutputFormat.setOutputPath(calculate, new Path(outputPath));
        calculate.setOutputFormatClass(TextOutputFormat.class);
        calculate.setOutputKeyClass(LongWritable.class);
        calculate.setOutputValueClass(LongWritable.class);
        calculate.setReducerClass(CalculateDegreeReducer.class);
        

        System.exit(calculate.waitForCompletion(true) ? 0 : 1);
	}
	public static class CalculateDegreeMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
	    @Override
	    public void map(LongWritable key, Text value, Context context)
	          throws IOException, InterruptedException {
      	  	  String[] info = value.toString().split("\t");
    		  String[] movies = info[1].split("\\|");
    		  context.write(new LongWritable(1), new LongWritable(movies.length));
	    }
	}
	public static class CalculateDegreeReducer extends Reducer<LongWritable,LongWritable,Text,Text> {
		@Override
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			long count = 0;
			for(LongWritable value: values) {
				++ count;
				sum += value.get();
			}
			System.out.println(sum + "/" + count + "=" + (double)sum / count);
		}
	}
}
