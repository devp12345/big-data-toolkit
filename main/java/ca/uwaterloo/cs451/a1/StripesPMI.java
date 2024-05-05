package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.pair.PairOfFloats;
import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.map.HMapStFW;
import tl.lin.data.map.HashMapWritable;
import tl.lin.data.map.MapKF;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;



public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);
  private static final int MAX_WORD_LENGTH = 40;

  private static final class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

          List<String> tokens = Tokenizer.tokenize(value.toString());
          Set<String> uniqueWordsSet = new HashSet<>();
          int len = Math.min(MAX_WORD_LENGTH, tokens.size());

          for(int i = 0; i < len; ++i){
            uniqueWordsSet.add(tokens.get(i));
          }

          List<String> uniqueWords = new ArrayList<>(uniqueWordsSet);
          
          for(int i = 0; i < uniqueWords.size(); ++i){
            WORD.set(uniqueWords.get(i));
            context.write(WORD, ONE);
          }

          // Lines
          WORD.set("*");
          context.write(WORD, ONE);
      
    }
  }

  private static final class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

          Iterator<IntWritable> iter = values.iterator();
          int sum = 0;
          while (iter.hasNext()) {
            sum += iter.next().get();
          }
            SUM.set(sum);
            context.write(key, SUM);
    }
  }

  private static final class Mapper2 extends Mapper<LongWritable, Text, Text, HMapStFW > {
        private static final Text KEY = new Text();


    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
          List<String> tokens = Tokenizer.tokenize(value.toString());
          Set<String> uniqueWordsSet = new HashSet<>();
          int len = Math.min(MAX_WORD_LENGTH, tokens.size());

          for(int i = 0; i < len; ++i){
            uniqueWordsSet.add(tokens.get(i));
          }

          List<String> uniqueWords = new ArrayList<>(uniqueWordsSet);

          for(int i = 0; i < uniqueWords.size() - 1; ++i){
            HMapStFW stripes = new HMapStFW();
            String currWord = uniqueWords.get(i);
            KEY.set(currWord);

            for(int j = 0; j < uniqueWords.size(); ++j){
              String iterWord = uniqueWords.get(j);
              if (i == j || iterWord.equals(currWord)) {
                    continue;
              }
              stripes.put(iterWord, 1f);
            }
            context.write(KEY, stripes);
          }
      
    }
  }

  private static final class Reducer2 extends Reducer<Text, HMapStFW, Text, HashMapWritable> {
    private static HashMap<String, Integer> frequency = new HashMap();
    private static final HashMapWritable<Text, PairOfFloatInt> RESULTS = new HashMapWritable<Text, PairOfFloatInt>();
    private static final PairOfFloatInt PAIR = new PairOfFloatInt();
    private int threshold;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        threshold = context.getConfiguration().getInt("threshold", 1);

        FileSystem fs = FileSystem.get(context.getConfiguration());
        String tempOutputPath = context.getConfiguration().get("tempOutputPath");
        int idx = 0;
        while(true){
         Path pass1 = new Path(tempOutputPath +"/part-r-0000" + idx);

        if (!fs.exists(pass1)) {
          break;
        }

        try {
            FSDataInputStream input = fs.open(pass1);
            InputStreamReader inStream = new InputStreamReader(input);

            BufferedReader reader = new BufferedReader(inStream);

            LOG.info("Reading from file " + pass1);

            String line = reader.readLine();
            while (line != null) {
                String[] word = line.split("\\s+");
                if (word.length == 2) {
                  frequency.put(word[0], Integer.parseInt(word[1]));
                    
                } else {
                    LOG.info("Input line not valid: '" + line + "'");
                }

                line = reader.readLine();
            }

            LOG.info("Finish reading file: " + pass1);
            reader.close();
           

        } catch (FileNotFoundException e) {
            LOG.error("Failed to open file: " + e.getMessage());
            break;
        }
        idx += 1;
        }
    }

    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context)
        throws IOException, InterruptedException {
          
              Iterator<HMapStFW> iter = values.iterator();
              HMapStFW map = new HMapStFW();

              while (iter.hasNext()) {
                map.plus(iter.next());
              }

              String keyString = key.toString();

              for (MapKF.Entry<String> entry : map.entrySet()) {
                String entryKey = entry.getKey();
                Integer entrySum = (int) entry.getValue();

                if(entrySum >= threshold){
                  Integer keyCount = frequency.get(keyString);
                  Integer entryKeyCount = frequency.get(entryKey);
                  Integer totalLines = frequency.get("*");
                  
                  if(totalLines != null && keyCount != null && entryKeyCount != null){
                    float PMI = (float) Math.log10((totalLines * entrySum)/(keyCount * entryKeyCount));
					          PAIR.set(PMI, entrySum);
                    RESULTS.put(new Text(entryKey), PAIR);
                  }

                }
              }

              context.write(key, RESULTS);
    }
  }

  private static final class MyCombiner extends Reducer<Text, HMapStFW, Text, HMapStFW> {
    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStFW> iter = values.iterator();
      HMapStFW map = new HMapStFW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      context.write(key, map);
    }
  }

  private StripesPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-threshold", metaVar = "[num]", usage = "minimum co-occurrence threshold")
    int threshold = 10;
  }

 
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool name: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);
    LOG.info(" - threshold: " + args.threshold);

    FileSystem fs = FileSystem.get(getConf());

    Path inputPath = new Path(args.input);
    Path tempOutputPath = new Path("temp_output"); // Temporary output for the first job
    Path finalOutputPath = new Path(args.output);
    

    Job job1 = Job.getInstance(getConf());
    job1.setJobName(StripesPMI.class.getSimpleName() + " - Pass 1");
    job1.setJarByClass(StripesPMI.class);

    job1.setNumReduceTasks(args.numReducers);
    job1.getConfiguration().setInt("threshold", args.threshold);

    FileInputFormat.setInputPaths(job1, inputPath);
    FileOutputFormat.setOutputPath(job1, tempOutputPath);

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    job1.setOutputFormatClass(TextOutputFormat.class);

    job1.setMapperClass(Mapper1.class);
    job1.setCombinerClass(Reducer1.class);
    job1.setReducerClass(Reducer1.class);

    job1.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job1.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job1.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    fs.delete(tempOutputPath, true);

    long startTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    System.out.println("Job1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");


    // JOB 2

    Job job2 = Job.getInstance(getConf());
    job2.setJobName(StripesPMI.class.getSimpleName() + " - Pass 2");
    job2.setJarByClass(StripesPMI.class);

    job2.setNumReduceTasks(args.numReducers);
    job2.getConfiguration().setInt("threshold", args.threshold);
    job2.getConfiguration().set("tempOutputPath", tempOutputPath.toString());

    FileInputFormat.setInputPaths(job2, inputPath);
    FileOutputFormat.setOutputPath(job2, finalOutputPath);

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(HMapStFW.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(HashMapWritable.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    job2.setMapperClass(Mapper2.class);
    job2.setCombinerClass(MyCombiner.class);
    job2.setReducerClass(Reducer2.class);

    job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    fs.delete(finalOutputPath, true);

    long secondStartTime = System.currentTimeMillis();
    job2.waitForCompletion(true);
    System.out.println("Job2 Finished in " + (System.currentTimeMillis() - secondStartTime) / 1000.0 + " seconds");

    System.out.println("Entire program finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    fs.delete(tempOutputPath, true);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}
