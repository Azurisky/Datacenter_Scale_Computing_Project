import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
//import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ItemClick {
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String s = new String();

    
  public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      	StringTokenizer itr = new StringTokenizer(value.toString());
      	while (itr.hasMoreTokens()) {
          	s = itr.nextToken();
			String[] arrOfStr = s.split(","); 
			String tmp = arrOfStr[1];
			tmp = tmp.split("T")[0];
			tmp = tmp.split("-")[1];
			if (!tmp.equals("04")) {
			  	continue;
			}
			word.set(arrOfStr[2]);
			context.write(word, one);
		}
    }
  }
  public static class IntSumReducer
        extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    private Configuration conf = new Configuration();
    private Text word = new Text();
    private Map<Integer,String> Top24Map = new TreeMap<Integer,String>(Collections.reverseOrder());
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
          sum += val.get();
        }
        Top24Map.put(sum, key.toString());
    }
    //The cleanup method is called once at the end of each task
    protected void cleanup(Context context) throws IOException, InterruptedException {
        conf.set("mapreduce.textoutputformat.separator", "\t");
        int count = 0;
        for(Map.Entry<Integer, String> m:Top24Map.entrySet()) {
        		if (count >= 10) {
        			break;
        		}
        	  	word.set(m.getValue());
        	  	result.set(m.getKey());
        	  	context.write(word, result);
        	  	count += 1;
        }
    }
    
  }
  
 
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(ItemClick.class);
    job.setMapperClass(TokenizerMapper.class);
//    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}