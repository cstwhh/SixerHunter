import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.TreeSet;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PreprocessRank {
	public static void main(String[] args) throws Exception {		
		
		Configuration conf = new Configuration();
		String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (remainingArgs.length != 2) {
			System.err.println("Usage: PreprocessRank <in> <out>");
			System.exit(2);
		}
		String inputPath = remainingArgs[0]; String outputPath = remainingArgs[1];

        Job preprocess = Job.getInstance(conf, "PreprocessRank");
        preprocess.setJarByClass(PreprocessRank.class);

        // Input / Mapper
        FileInputFormat.addInputPath(preprocess, new Path(inputPath));
        preprocess.setMapOutputKeyClass(Text.class);
        preprocess.setMapperClass(PreprocessRankMapper.class);

        // Output / Reducer
        // 判断output文件夹是否存在，如果存在则删除  
        Path path = new Path(outputPath);
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(path)) {  
            fileSystem.delete(path, true);
        }  
        FileOutputFormat.setOutputPath(preprocess, new Path(outputPath));
        preprocess.setOutputFormatClass(TextOutputFormat.class);
        preprocess.setOutputKeyClass(Text.class);
        preprocess.setOutputValueClass(Text.class);
//        preprocess.setReducerClass(PreprocessReducer.class);
        preprocess.setNumReduceTasks(0);

        System.exit(preprocess.waitForCompletion(true) ? 0 : 1);
	}
	public static class PreprocessRankMapper extends Mapper<LongWritable, Text, Text, Text> {
	    private static final String rankPattern = "  <\\d*?>";
	    @Override
	    public void map(LongWritable key, Text value, Context context)
	          throws IOException, InterruptedException {
      	  	  String[] info = value.toString().split("\t");
      	  	  if(info == null || info.length < 2) return;
    		  String actor = info[0];	
    		  if(! isPureAscii(actor)) return;
    		  if(actor.contains("|")) {System.err.println("actor[" + actor + "] contains |");System.exit(1);}
    		  TreeSet<String> movies = new TreeSet<String>();
    		  TreeSet<String> moviesName = new TreeSet<String>();
    		  for(int i = 1; i < info.length; ++i) {
    			  if(info[i].equals("") || (!isPureAscii(info[i]))) continue;
        		  if(info[i].contains("|")) {System.err.println("movie[" + actor + "] contains |");System.exit(1);}
        		  String name = info[i].replaceAll(rankPattern, "");
//        		  System.out.println("info[i]: " + info[i] + ", name: " + name);
        		  if(!moviesName.contains(name)) {
        			  moviesName.add(name);
        			  movies.add(info[i]);
        		  }
    		  }
    		  if(movies.size() < 1) return;
    		  String[] movieList = new String[movies.size()];
    		  movieList = movies.toArray(movieList);
    		  context.write(new Text(actor), new Text(StringUtils.join(movieList, "|")));
	    }
	}
	  static CharsetEncoder asciiEncoder = Charset.forName("US-ASCII").newEncoder(); // or "ISO-8859-1" for ISO Latin 1
	  public static boolean isPureAscii(String v) {
	    return asciiEncoder.canEncode(v);
	  }
}
