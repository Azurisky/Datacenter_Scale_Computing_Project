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

public class TimeBlocks {
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
          tmp = tmp.split("T")[1];
          tmp = tmp.split(":")[0];
          int price = Integer.parseInt(arrOfStr[3]);
          int quantity = Integer.parseInt(arrOfStr[4]);
          int revenue = price*quantity;
          IntWritable R = new IntWritable(revenue);
        word.set(tmp);
        context.write(word, R);
      }
    }
  
   public String rmNoise(String s) {
      char[] chars = s.toCharArray();
      StringBuilder sb = new StringBuilder(); 
      for (char c : chars) {
      if (Character.isLetter(c)) {
        sb.append(c);
      }
      }
      return sb.toString();
    }
  }
  public static class IntSumReducer
        extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    private Configuration conf = new Configuration();
    private Text word = new Text();
    private Map<String,Integer> Top24MapR = new HashMap<String,Integer>();
    private Map<Integer,String> Top24Map = new TreeMap<Integer,String>(Collections.reverseOrder());
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
          sum += val.get();
        }
        Top24MapR.put(key.toString(), sum);
        Top24Map.put(sum, key.toString());
//      result.set(sum);
//      context.write(key, result);
    }
    //The cleanup method is called once at the end of each task
    protected void cleanup(Context context) throws IOException, InterruptedException {
        conf.set("mapreduce.textoutputformat.separator", "\t");
//        for(Map.Entry<String, Integer> m:Top24MapR.entrySet()) {
//          word.set(m.getKey());
//          context.write(word, new IntWritable(m.getValue()));
//    }
        
        for(Map.Entry<Integer, String> m:Top24Map.entrySet()) {
          word.set(m.getValue());
          context.write(word, new IntWritable(m.getKey()));
        }
    }
    
  }
  
 
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(TimeBlocks.class);
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