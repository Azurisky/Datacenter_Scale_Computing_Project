import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
//import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SuccessRate {
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable buy = new IntWritable(1);
    private final static IntWritable click = new IntWritable(10);
    private Text word = new Text();
    private Text val = new Text();
    private String s = new String();

    
  public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            s = itr.nextToken();
      String[] arrOfStr = s.split(","); 
      if (arrOfStr.length == 4) {
        word.set(arrOfStr[2]);
        context.write(word, click);
      } else {
        word.set(arrOfStr[2]);
        context.write(word, buy);
      }
      
    }
    }
  }
  public static class IntSumReducer
        extends Reducer<Text,IntWritable,Text,FloatWritable> {
    private FloatWritable result = new FloatWritable();
    private Configuration conf = new Configuration();
    private Text word = new Text();
    private Map<Float, ArrayList<String>> Top24Map = new TreeMap<Float, ArrayList<String>>(Collections.reverseOrder());
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
        float sumC = 0;
        float sumB = 0;
        float rate = 0;
        for (IntWritable val : values) {
            if (val.get() == 1) {
                sumB ++;
              } else {
                  sumC ++;
              }
        }
        rate = sumB/sumC;
        ArrayList<String> ar = new ArrayList<String>();
        if (!Top24Map.containsKey(rate)) {
            Top24Map.put(rate, new ArrayList<String>());
        }
        Top24Map.get(rate).add(key.toString());
    }
    //The cleanup method is called once at the end of each task
    protected void cleanup(Context context) throws IOException, InterruptedException {
        conf.set("mapreduce.textoutputformat.separator", "\t");
        int count = 0;
        
        for(Map.Entry<Float, ArrayList<String>> m:Top24Map.entrySet()) {
            for (String s:m.getValue()) {
                if (count >= 10) {
                  break;
                }
                word.set(s);
                result.set(m.getKey());
                context.write(word, result);
                count += 1;
            } 
        }
    }
    
  }
  
 
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(SuccessRate.class);
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