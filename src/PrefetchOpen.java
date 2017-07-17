import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.StringUtils;

// 保存open的数据，alldata只发送open的数据。如果open数据通过setup设置性能反而下降，那么就不再保存open数据
public class PrefetchOpen {
	public final static int DEPTH = 13;
//	public final static String source = "1";public final static String dest = "18";
	public final static String source = "Bernardo, Alecia"; public final static String dest = "Boyer, Erica";
//	public final static String source = "Bernardo, Alecia"; public final static String dest = "Boyer, Ericdsadasa";
	public final static boolean cacheAll = true; 
	public final static int MAX_CACHE_DEPTH = 5;
//	public final static int MAX_CACHE_DEPTH = 20;
	
	public final static String resultFile = "result";
	public final static String onlyOpenNodes = "o";
	public final static String allNodes = "a";
	// 表示只发送openNodes之中的节点的children
//	public static boolean onlyOpenNodes = true;
	public static void main(String[] args) throws Exception {		
		Configuration conf = new Configuration();
        conf.set("source", source);
        conf.set("dest", dest);
        conf.set("resultFile", resultFile);
        conf.set("nodes", onlyOpenNodes);
        conf.set("writenodes", onlyOpenNodes);
		String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (remainingArgs.length != 3) {
			System.err.println("Usage: PrefetchOpen <data> <inverted> <cachePath>");
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
			if(i == MAX_CACHE_DEPTH) {
		        conf.set("writenodes", allNodes);
			}
    	    if(i > MAX_CACHE_DEPTH) {
    	        conf.set("nodes", allNodes);
    	    }
	        Job bfs = Job.getInstance(conf, "PrefetchOpen");
	        bfs.setJarByClass(PrefetchOpen.class);
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
	        if(i != 1) {
	        	String inputPath = cachePath + (i-1);
	        	if(cacheAll) inputPath = cachePath + (i-1);
	        	else inputPath = cachePath + ((i-1)%2);
		        MultipleInputs.addInputPath(bfs, new Path(inputPath + "/part-r-00000"), TextInputFormat.class,PrefetchOpenMapper.class);
		        if(i <= MAX_CACHE_DEPTH) {
			        Path openNodePath = new Path(inputPath + "/opendata-r-00000");
			        if(openNodePath.getFileSystem(conf).exists(openNodePath)) {
			        	bfs.addCacheFile(openNodePath.toUri());
			        }
		        }
	        }
	        MultipleInputs.addInputPath(bfs, new Path(dataPath), TextInputFormat.class,
	        		PrefetchOpenMapper.class);
	        MultipleInputs.addInputPath(bfs, new Path(invertedDataPath), TextInputFormat.class,
	        		PrefetchOpenMapper.class);
	        bfs.setMapOutputKeyClass(Text.class);
	        
	        String outputPath;
	        if(cacheAll) outputPath = cachePath + i + "/";
	        else outputPath = cachePath + (i%2) + "/";
	        Path path = new Path(outputPath);
	        FileSystem fileSystem = path.getFileSystem(conf);
	        if (fileSystem.exists(path)) {  
	            fileSystem.delete(path, true);
	        }  
	        MultipleOutputs.addNamedOutput(bfs, "opendata",   TextOutputFormat.class, Text.class, Text.class);
	        FileOutputFormat.setOutputPath(bfs, new Path(outputPath));
	        bfs.setOutputFormatClass(TextOutputFormat.class);
	        bfs.setOutputKeyClass(Text.class);
	        bfs.setOutputValueClass(Text.class);
	        bfs.setReducerClass(PrefetchOpenReducer.class);
	        bfs.waitForCompletion(true);
	        long endTime = System.currentTimeMillis();
			System.err.println("Depth: " + i + ", Time: " + (endTime - startTime) + "ms");
	        System.out.print((endTime - startTime) + "ms;");
		}
	}
	public static class PrefetchOpenMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Set<String> openNodes = new HashSet<String>();
		private Configuration conf;
		private boolean inOnlyOpenNodesMode = false;
		private boolean inAllNodesMode = false;
	    @Override
	    public void setup(Context context) throws IOException,InterruptedException {
    		conf = context.getConfiguration();
    		if(conf.get("nodes").equals(onlyOpenNodes)) inOnlyOpenNodesMode = true;
    		else inAllNodesMode = true;
	        URI[] openURIs = Job.getInstance(conf).getCacheFiles();
	        if(openURIs == null) return;
	        for (URI openURI : openURIs) {
	          Path openPath = new Path(openURI.getPath());
	          String fileName = openPath.toString();
	          try {
	        	  BufferedReader fis = new BufferedReader(new FileReader(fileName));
		          String openNode = null;
		          int lineCount = 0;
		          while ((openNode = fis.readLine()) != null) {
		        	  openNodes.add(openNode.replace("\t", ""));
//		        	  System.out.println("addOpenNode: " + openNode.replace("\t", ""));
		        	  ++ lineCount;
		          }
		          fis.close();
//		          if(openNodes.size() > MAX_CACHE_SIZE) onlyOpenNodes = false;
//		          System.out.println("Read " + lineCount + " lines, size: " + openNodes.size());
//		          System.err.println("file: " + fileName + ", Read " + lineCount + " lines, size: " + openNodes.size() + ", openNodes: " + openNodes.toString());
		          System.err.println("Read " + lineCount + " lines, size: " + openNodes.size());
		        } catch (IOException ioe) {
		          System.err.println("Caught exception while parsing the cached file '"
		              + StringUtils.stringifyException(ioe));
		        }
	        }
	    }
	    @Override
	    public void map(LongWritable key, Text value, Context context)
	          throws IOException, InterruptedException {
	    	  //  alldata:			<name children>
	    	  //  cachedata.open:	<name children distance parent>
	          //  cachedata.close:	<name>
//	    	System.out.println("Map receive: " + value.toString());
      	  	  String[] info = value.toString().split("\t");
    		  String name = info[0];
    		  // all data
    		  if(info.length == 2) {
    			  // alldata.source
    			  if(name.equals(conf.get("source"))) {
    				  // <name,distance parent>，fake自己是被父节点open的
    				  context.write(new Text(name), new Text("0\t" + name));
	    			  // 必须发送自己<name,children>
    				  context.write(new Text(name), new Text(info[1]));
    			  }
    			  // alldata.other <name,children> 
    			  else {
    				  if(inOnlyOpenNodesMode && openNodes.contains(name)) {
//    					  System.out.println("Map: OnlyOpenNodes: " + name);
        				  context.write(new Text(name), new Text(info[1]));
    				  }
    				  else if(inAllNodesMode){
//    					  System.out.println("Map: SendAllNodes: " + name);
        				  context.write(new Text(name), new Text(info[1]));
    				  }
    			  }
    		  }
    		  // cachedata.open
    		  else if(info.length == 4){
        		  String[] children = info[1].split("\\|");
    			  String distance = info[2];
    			  String parent = info[3];
				  // 按照cachedata.open的发送规则<child,distance,parent>
//    			  if(name.equals(conf.get("source"))) parent = name;else parent = (parent + "|" + name);
    			  parent = (parent + "|" + name);
    			  for(int k = 0; k < children.length; ++k) {
    				  context.write(new Text(children[k]), new Text(distance + "\t" + parent));
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
	public static class PrefetchOpenReducer extends Reducer<Text,Text,Text,Text> {
		private MultipleOutputs<Text, Text> mo;
        @Override
        protected void setup(Context context) throws IOException,
        InterruptedException {
            mo = new MultipleOutputs<Text, Text>(context);
            super.setup(context);
        }
        
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// 来自self.alldata的数据
			String children = "";
			boolean hasChildren = false;
			// 来自self.close的数据
			boolean isClosed = false;
			// 来自parent的数据
			boolean isOpening = false;
			int parentsDistance = Integer.MAX_VALUE;
			String parentsParent = "";
			
			for(Text value: values) {
//				System.out.println("reduce receive [" + key.toString() + "] " + value);
				String[] info = value.toString().split("\t");
				// self.close
				if(value.toString().equals("<c>")) {
					isClosed = true;
				}
				// self.alldata
				else if(info.length == 1) {
					children = value.toString();
					hasChildren = true;
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
				if(hasChildren) {
					context.write(key, new Text(children + "\t" + parentsDistance + "\t" + parentsParent));
	
					if(context.getConfiguration().get("writenodes").equals(onlyOpenNodes)) {
	//					System.out.println("Reduce: OnlyOpenNodes " + key.toString());
						String[] childs = children.split("\\|");
//						for(String child: ) {
		    			 for(int k = 0; k < childs.length; ++k) {
	//						if(child.length() ==0) continue;
							mo.write("opendata", new Text(childs[k]), new Text(""));
						}
					}
				}
			}
			
		}
        @Override
        protected void cleanup(Context context) throws IOException,InterruptedException{
            mo.close();
            super.cleanup(context);
        }
	}
}
