import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.util.LineReader;

// 只缓存open节点的全部数据，close节点只保留一个name
public class BFSCacheOpen {
	public final static int DEPTH = 12;
//	public final static String source = "1";public final static String dest = "8";
	public final static String source = "Bernardo, Alecia"; public final static String dest = "Boyer, Erica";
//	public final static String source = "Bernardo, Alecia"; public final static String dest = "Boyer, Ericdsadasa";
	public final static boolean cacheAll = false; 
	
	public final static String resultFile = "result";
	public static void main(String[] args) throws Exception {		
		
		Configuration conf = new Configuration();
        conf.set("source", source);
        conf.set("dest", dest);
        conf.set("resultFile", resultFile);
		String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (remainingArgs.length != 3) {
			System.err.println("Usage: BFS <data> <inverted> <cachePath>");
			System.exit(2);
		}

    	FileSystem fs = FileSystem.get(conf); 
    	Path resultPath = new Path(resultFile);  
    	if (fs.exists(resultPath)) {
    		fs.delete(resultPath, true);
    	}
		String dataPath = remainingArgs[0]; 
		String invertedDataPath = remainingArgs[1]; 
		String cachePath = remainingArgs[2];
		for(int i = 1;i <= DEPTH; ++i) {
			long startTime = System.currentTimeMillis();
	        Job bfs = Job.getInstance(conf, "BFS");
	        bfs.setJarByClass(BFS.class);
	        // 已经写入结果了，则可以退出
        	if (fs.exists(resultPath)) {
        	    FSDataInputStream fin = fs.open(resultPath);
        	    Text line = new Text();
        	    LineReader reader = new LineReader(fin);
        	    if(reader.readLine(line) > 0) {
        	    	System.err.println("result: " + line);
        	    	System.exit(0);
        	    } 
        	    reader.close();
        	    fin.close();
    	    }

    	    
	        MultipleInputs.addInputPath(bfs, new Path(dataPath), TextInputFormat.class,
	        		BFSMapper.class);
	        MultipleInputs.addInputPath(bfs, new Path(invertedDataPath), TextInputFormat.class,
	        		BFSMapper.class);
	        if(i != 1) {
		        if(cacheAll) MultipleInputs.addInputPath(bfs, new Path(cachePath + (i-1) + "/"), TextInputFormat.class,BFSMapper.class); 
		        else MultipleInputs.addInputPath(bfs, new Path(cachePath + ((i-1)%2) + "/"), TextInputFormat.class,BFSMapper.class);
		        
	        }
	        bfs.setMapOutputKeyClass(Text.class);
	        
	        String outputPath;
	        if(cacheAll) outputPath = cachePath + i + "/";
	        else outputPath = cachePath + (i%2) + "/";
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
	        bfs.waitForCompletion(true);
	        long endTime = System.currentTimeMillis();
			System.err.println("Depth: " + i + ", Time: " + (endTime - startTime) + "ms");
	        System.out.print((endTime - startTime) + "ms;");
		}
	}
	public static class BFSMapper extends Mapper<LongWritable, Text, Text, Text> {
	    @Override
	    public void map(LongWritable key, Text value, Context context)
	          throws IOException, InterruptedException {
	    	  //  alldata:			<name children>
	    	  //  cachedata.open:	<name children distance parent>
	          //  cachedata.close:	<name>
      	  	  String[] info = value.toString().split("\t");
    		  String name = info[0];
    		  // all data
    		  if(info.length == 2) {
        		  String[] children = info[1].split("\\|");
    			  // alldata.source
    			  if(name.equals(context.getConfiguration().get("source"))) {
    				  // 按照cachedata.open的发送规则<child,distance parent>
	    			  for(String child: children) {
	    				  context.write(new Text(child), new Text("0\t" + name));
	    			  }
	    			  // 按照cachedata.all的发送规则<name,"<c>"> 
	    			  context.write(new Text(name), new Text("<c>"));
    			  }
    			  // alldata.other <name,children>
    			  else {
    				  context.write(new Text(name), new Text(info[1]));
    			  }
    		  }
    		  // cachedata.open
    		  else if(info.length == 4){
        		  String[] children = info[1].split("\\|");
    			  String distance = info[2];
    			  String parent = info[3];
				  // 按照cachedata.open的发送规则<child,distance,parent>
				  parent = (parent + "|" + name);
    			  for(String child: children) {
    				  context.write(new Text(child), new Text(distance + "\t" + parent));
    			  }
    			  // 按照cachedata.all的发送规则<name,"<c>"> 
    			  context.write(new Text(name), new Text("<c>"));
    		  }
    		  // cache.close也按照cache.all的发送规则
    		  else if(info.length == 1){
    			  // 按照cachedata.all的发送规则<name,"<c>"> 
    			  context.write(new Text(name), new Text("<c>"));
    		  }
    		  
	    }
	}
	public static class BFSReducer extends Reducer<Text,Text,Text,Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// 来自self.alldata的数据
			String children = "";
			// 来自self.close的数据
			boolean isClosed = false;
			// 来自parent的数据
			boolean isOpening = false;
			int parentsDistance = Integer.MAX_VALUE;
			String parentsParent = "";
			
			for(Text value: values) {
//				System.out.println("reduce received [" + key.toString() + "] " + value);
				String[] info = value.toString().split("\t");
				// self.close
				if(value.toString().equals("<c>")) {
					isClosed = true;
				}
				// self.alldata
				else if(info.length == 1) {
					children = value.toString();
				}
				// parent:parent's<distance,parent>
				else if(info.length == 2 && !isClosed) {
					isOpening = true;
					int distance = Integer.valueOf(info[0]);
					if(distance + 1 < parentsDistance) {
						parentsDistance = distance + 1;
						parentsParent = info[1];
					}
				}
			}
			// close节点
			if(isClosed) {
				context.write(key, new Text(""));
			}
			// open节点
			else if(isOpening) {
				if(key.toString().equals(context.getConfiguration().get("dest"))) {
			        String result = parentsParent + "|" + key.toString();  
			        Configuration config = context.getConfiguration();
			        
		        	FileSystem fs = FileSystem.get(config); 
		        	Path filenamePath = new Path(config.get("resultFile"));  
		        	if (fs.exists(filenamePath)) {
	        	        fs.delete(filenamePath, true);
	        	    }

	        	    FSDataOutputStream fout = fs.create(filenamePath);
	        	    fout.writeBytes(result);
	        	    fout.close();
					return;
				}
				context.write(key, new Text(children + "\t" + parentsDistance + "\t" + parentsParent));
			}
		}
	}
}
