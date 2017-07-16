import java.io.IOException;
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

public class BFS {
	public final static int DEPTH = 1;
	public final static String source = "1";
	public final static String dest = "10";
	public static void main(String[] args) throws Exception {		
		
		Configuration conf = new Configuration();
		String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (remainingArgs.length != 3) {
			System.err.println("Usage: BFS <data> <inverted> <cachePath>");
			System.exit(2);
		}
		String dataPath = remainingArgs[0]; 
		String invertedDataPath = remainingArgs[1]; 
		String cachePath = remainingArgs[2];
		for(int i = 1;i < DEPTH; ++i) {
	        Job bfs = Job.getInstance(conf, "BFS");
	        bfs.setJarByClass(BFS.class);
	        bfs.getConfiguration().set("source", source);
	        bfs.getConfiguration().set("dest", dest);
	        String result = bfs.getConfiguration().get("result", "");
	        if(!result.equals("")) {
	        	System.out.print(result);
	        	return;
	        }
	        MultipleInputs.addInputPath(bfs, new Path(dataPath), TextInputFormat.class,
	        		BFSMapper.class);
	        MultipleInputs.addInputPath(bfs, new Path(invertedDataPath), TextInputFormat.class,
	        		BFSMapper.class);
	        if(i != 1) {
		        MultipleInputs.addInputPath(bfs, new Path(cachePath + (i-1) + "/"), TextInputFormat.class,
		        		BFSMapper.class);
	        }
	        bfs.setMapOutputKeyClass(Text.class);
	        
	        String outputPath = cachePath + i + "/";
	        Path path = new Path(outputPath);
	        FileSystem fileSystem = path.getFileSystem(conf);
	        if (fileSystem.exists(path)) {  
	            fileSystem.delete(path, true);
	        }  
	        
	        FileOutputFormat.setOutputPath(bfs, new Path(outputPath));
	        bfs.setOutputFormatClass(TextOutputFormat.class);
	        bfs.setOutputKeyClass(Text.class);
	        bfs.setOutputValueClass(Text.class);
	        bfs.setReducerClass(BFSReducer.class);
	        System.exit(bfs.waitForCompletion(true) ? 0 : 1);
		}
	}
	public static class BFSMapper extends Mapper<LongWritable, Text, Text, Text> {
	    @Override
	    public void map(LongWritable key, Text value, Context context)
	          throws IOException, InterruptedException {
      	  	  String[] info = value.toString().split("\t");
    		  String name = info[0];
    		  String[] children = info[1].split("\\|");
    		  // all data
    		  if(info.length == 2) {
    			  // alldata.source
    			  if(name.equals(context.getConfiguration().get("source"))) {
    				  // 按照cachedata.open的发送规则
	    			  for(String child: children) {
	    				  context.write(new Text(child), new Text("0\t" + name));
	    			  }
	    			  // 更改自己的状态为close，按照cachedata.all的发送规则?
	    			  context.write(new Text(name), new Text("0\tC" + name));
    			  }
    			  // alldata.others
    			  else {
    				  context.write(new Text(name), new Text(info[1]));
    			  }
    		  }
    		  // cache data
    		  else {
    			  String distance = info[2];
    			  String status = info[3];
    			  String parent = info[4];
    			  if(status.equals("O")) {
    				  // 按照cachedata.open的发送规则
	    			  for(String child: children) {
	    				  context.write(new Text(child), new Text(distance + "\t" + parent + "|" + name));
	    			  }
	    			  // 更改自己的状态为close，按照cachedata.all的发送规则
	    			  status = "C";
    			  }
    			  // 按照cachedata.all的发送规则
    			  context.write(new Text(name), new Text(distance + status + parent));
    		  }
	    }
	}
	public static class BFSReducer extends Reducer<Text,Text,Text,Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String children = "";
			// 存储从自身接收到的cache数据
			boolean selfHasCached = false;
			String selfDistance = "";
			String selfStatus = "";
			String selfParent = "";
			// 存储从父节点接收到的cache数据
			boolean newCache = false;
			int parentsDistance = Integer.MAX_VALUE;
			String parentsParent = "";
			
			for(Text value: values) {
				String[] info = value.toString().split("\t");
				// self.alldata
				if(info.length == 0) {
					children = value.toString();
				}
				// self.cachedata
				else if(info.length == 3) {
					selfHasCached = true;
					selfDistance = info[0];
					selfStatus = info[1];
					selfParent = info[2];
				}
				// parent
				else if(info.length == 2 && !selfHasCached) {
					newCache = false;
					int distance = Integer.valueOf(info[0]);
					if(distance + 1 < parentsDistance) {
						parentsDistance = distance + 1;
						parentsParent = info[1];
					}
				}
			}
			// 上一轮已经缓存了，处于close状态
			if(selfHasCached) {
				context.write(key, new Text(children + "\t" + selfDistance + "\t" + selfStatus + "\t" + selfParent));
			}
			else if(newCache) {
				context.write(key, new Text(children + "\t" + parentsDistance + "\t" + "O" + "\t" + parentsParent));
			}
		}
	}
}
