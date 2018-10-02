import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
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

public class WordCountLarge {
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String s = new String();
    private Set<String> stopWords;
    public TokenizerMapper() {
      super();
      stopWords = new HashSet<String>();
      stopWords.add("a");
      stopWords.add("about");
      stopWords.add("above");
      stopWords.add("after");
      stopWords.add("again");
      stopWords.add("against");
      stopWords.add("all");
      stopWords.add("am");
      stopWords.add("an");
      stopWords.add("and");
      stopWords.add("any");
      stopWords.add("are");
      stopWords.add("arent");
      stopWords.add("as");
      stopWords.add("at");
      stopWords.add("be");
      stopWords.add("because");
      stopWords.add("been");
      stopWords.add("before");
      stopWords.add("being");
      stopWords.add("below");
      stopWords.add("between");
      stopWords.add("both");
      stopWords.add("but");
      stopWords.add("by");
      stopWords.add("cant");
      stopWords.add("cannot");
      stopWords.add("could");
      stopWords.add("couldnt");
      stopWords.add("did");
      stopWords.add("didnt");
      stopWords.add("do");
      stopWords.add("does");
      stopWords.add("doesnt");
      stopWords.add("doing");
      stopWords.add("dont");
      stopWords.add("down");
      stopWords.add("during");
      stopWords.add("each");
      stopWords.add("few");
      stopWords.add("for");
      stopWords.add("from");
      stopWords.add("further");
      stopWords.add("had");
      stopWords.add("hadnt");
      stopWords.add("has");
      stopWords.add("hasnt");
      stopWords.add("have");
      stopWords.add("havent");
      stopWords.add("having");
      stopWords.add("he");
      stopWords.add("hed");
      stopWords.add("hell");
      stopWords.add("hes");
      stopWords.add("her");
      stopWords.add("here");
      stopWords.add("heres");
      stopWords.add("hers");
      stopWords.add("herself");
      stopWords.add("him");
      stopWords.add("himself");
      stopWords.add("his");
      stopWords.add("how");
      stopWords.add("hows");
      stopWords.add("i");
      stopWords.add("id");
      stopWords.add("ill");
      stopWords.add("im");
      stopWords.add("ive");
      stopWords.add("if");
      stopWords.add("in");
      stopWords.add("into");
      stopWords.add("is");
      stopWords.add("isnt");
      stopWords.add("it");
      stopWords.add("its");
      stopWords.add("its");
      stopWords.add("itself");
      stopWords.add("lets");
      stopWords.add("me");
      stopWords.add("more");
      stopWords.add("most");
      stopWords.add("mustnt");
      stopWords.add("my");
      stopWords.add("myself");
      stopWords.add("no");
      stopWords.add("nor");
      stopWords.add("not");
      stopWords.add("of");
      stopWords.add("off");
      stopWords.add("on");
      stopWords.add("once");
      stopWords.add("only");
      stopWords.add("or");
      stopWords.add("other");
      stopWords.add("ought");
      stopWords.add("our");
      stopWords.add("ours");
      stopWords.add("ourselves");
      stopWords.add("out");
      stopWords.add("over");
      stopWords.add("own");
      stopWords.add("same");
      stopWords.add("shant");
      stopWords.add("she");
      stopWords.add("shed");
      stopWords.add("shell");
      stopWords.add("shes");
      stopWords.add("should");
      stopWords.add("shouldnt");
      stopWords.add("so");
      stopWords.add("some");
      stopWords.add("such");
      stopWords.add("than");
      stopWords.add("that");
      stopWords.add("thats");
      stopWords.add("the");
      stopWords.add("their");
      stopWords.add("theirs");
      stopWords.add("them");
      stopWords.add("themselves");
      stopWords.add("then");
      stopWords.add("there");
      stopWords.add("theres");
      stopWords.add("these");
      stopWords.add("they");
      stopWords.add("theyd");
      stopWords.add("theyll");
      stopWords.add("theyre");
      stopWords.add("theyve");
      stopWords.add("this");
      stopWords.add("those");
      stopWords.add("through");
      stopWords.add("to");
      stopWords.add("too");
      stopWords.add("under");
      stopWords.add("until");
      stopWords.add("up");
      stopWords.add("very");
      stopWords.add("was");
      stopWords.add("wasnt");
      stopWords.add("we");
      stopWords.add("wed");
      stopWords.add("well");
      stopWords.add("were");
      stopWords.add("weve");
      stopWords.add("were");
      stopWords.add("werent");
      stopWords.add("what");
      stopWords.add("whats");
      stopWords.add("when");
      stopWords.add("whens");
      stopWords.add("where");
      stopWords.add("wheres");
      stopWords.add("which");
      stopWords.add("while");
      stopWords.add("who");
      stopWords.add("whos");
      stopWords.add("whom");
      stopWords.add("why");
      stopWords.add("whys");
      stopWords.add("with");
      stopWords.add("wont");
      stopWords.add("would");
      stopWords.add("wouldnt");
      stopWords.add("you");
      stopWords.add("youd");
      stopWords.add("youll");
      stopWords.add("youre");
      stopWords.add("youve");
      stopWords.add("your");
      stopWords.add("yours");
      stopWords.add("yourself");
      stopWords.add("yourselves");
    }
    
  public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
          s = rmNoise(itr.nextToken());
          s = s.toLowerCase();
          if (s.equals("") || stopWords.contains(s)) {
            continue;
          }
        word.set(s);
        context.write(word, one);
      }
    }
  
//  public Set<String> readFile(String f) {
//      Set<String> set = new HashSet<String>();
//      
//      FileReader file = new FileReader(f); 
//      BufferedReader br = new BufferedReader(file);
//      String line;
//      while ((line = br.readLine()) != null) {
//        set.add(line);
//        System.out.println(line);
//      }
//      
//      return set;
//    }
  
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
    private Text word = new Text();
    private Map<Integer, ArrayList<String>> Top2000Map = new TreeMap<Integer, ArrayList<String>>(Collections.reverseOrder());
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      ArrayList<String> ar = new ArrayList<String>();
      if (!Top2000Map.containsKey(sum)) {
          Top2000Map.put(sum, new ArrayList<String>());
      }
      Top2000Map.get(sum).add(key.toString());
//      result.set(sum);
//      context.write(key, result);
    }
    
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
//        for(Map.Entry<String, Integer> m:Top24MapR.entrySet()) {
//          word.set(m.getKey());
//          context.write(word, new IntWritable(m.getValue()));
//    }
        int count = 0;
        for(Map.Entry<Integer, ArrayList<String>> m:Top2000Map.entrySet()) {
            for (String s:m.getValue()) {
              if (count >= 2000) {
                break;
              }
              word.set(s);
                context.write(word, new IntWritable(m.getKey()));
                count += 1;
            }
        }
    }
  }
 
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCountLarge.class);
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