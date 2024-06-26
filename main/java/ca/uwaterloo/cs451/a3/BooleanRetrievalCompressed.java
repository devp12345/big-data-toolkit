package ca.uwaterloo.cs451.a3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.Integer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.ArrayList;

public class BooleanRetrievalCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BooleanRetrievalCompressed.class);
  private MapFile.Reader[] index;
  private FSDataInputStream collection;
  private Stack<Set<Integer>> stack;
  private int numMappers;

  private BooleanRetrievalCompressed() {}

  private void initialize(String indexPath, String collectionPath, FileSystem fs) throws IOException {

    String prefix = "/part-r-";
    
    FileStatus[] files = fs.listStatus(new Path(indexPath));
    List<Path> targetPaths = new ArrayList<>();

    for (FileStatus file : files) {
        if (file.getPath().toString().contains(prefix)) {
            targetPaths.add(file.getPath());
        }
    }

    numMappers = targetPaths.size();
    LOG.info("Num Mappers: " + Integer.toString(numMappers));

    Collections.sort(targetPaths); 

    index = new MapFile.Reader[numMappers];

    int i = 0;
    for (Path dataPath : targetPaths) {
        if (fs.exists(dataPath)) {
            index[i++] = new MapFile.Reader(dataPath, fs.getConf());
            LOG.info("Adding file reader for: " + dataPath);
        }
    }

    collection = fs.open(new Path(collectionPath));
    stack = new Stack<>();
  }

  private void runQuery(String q) throws IOException {
    String[] terms = q.split("\\s+");

    for (String t : terms) {
      if (t.equals("AND")) {
        performAND();
      } else if (t.equals("OR")) {
        performOR();
      } else {
        pushTerm(t);
      }
    }

    Set<Integer> set = stack.pop();

    for (Integer i : set) {
      String line = fetchLine(i);
      System.out.println(i + "\t" + line);
    }
  }

  private void pushTerm(String term) throws IOException {
    stack.push(fetchDocumentSet(term));
  }

  private void performAND() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<>();

    for (int n : s1) {
      if (s2.contains(n)) {
        sn.add(n);
      }
    }

    stack.push(sn);
  }

  private void performOR() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<>();

    for (int n : s1) {
      sn.add(n);
    }

    for (int n : s2) {
      sn.add(n);
    }

    stack.push(sn);
  }

  private Set<Integer> fetchDocumentSet(String term) throws IOException {
    Set<Integer> set = new TreeSet<>();

    for (PairOfInts pair : fetchPostings(term)) {
      set.add(pair.getLeftElement());
    }

    return set;
  }

  private ArrayListWritable<PairOfInts> fetchPostings(String term) throws IOException {
    BytesWritable bytes = new BytesWritable();
    int partition = (term.hashCode() & Integer.MAX_VALUE) % numMappers;
    index[partition].get(new Text(term), bytes);
    return parseBytes(bytes.getBytes());
  }

  private ArrayListWritable<PairOfInts> parseBytes(byte[] rawBytes) throws IOException {
    ByteArrayInputStream byteStream = new ByteArrayInputStream(rawBytes);
    DataInputStream inputStream = new DataInputStream(byteStream);

    ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();
    int docNum = 0;
    int tf = 0;
    int readCount = 0;
    int DF = 0;

    // docno, tf, docno, tf, ..., docno, tf, DF
    while (inputStream.available() > 0) {
        int value = WritableUtils.readVInt(inputStream);
        readCount++;

        // If we have read an odd number and are at the end of the stream its DF
        if (readCount % 2 == 1 && inputStream.available() == 0) {
            DF = value;
            break;
        } else if (readCount % 2 == 1) { // Odd number of reads and not end is docNum
            docNum += value;
        } else { // Even number of reads is tf
            tf = value;
            postings.add(new PairOfInts(docNum, tf));
        }
    }

    return postings;
}


  public String fetchLine(long offset) throws IOException {
    collection.seek(offset);
    BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

    String d = reader.readLine();
    return d.length() > 80 ? d.substring(0, 80) + "..." : d;
  }

  private static final class Args {
    @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
    String index;

    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
    String collection;

    @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
    String query;
  }

  /**
   * Runs this tool.
   */
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

    if (args.collection.endsWith(".gz")) {
      System.out.println("gzipped collection is not seekable: use compressed version!");
      return -1;
    }

    FileSystem fs = FileSystem.get(new Configuration());

    initialize(args.index, args.collection, fs);

    System.out.println("Query: " + args.query);
    long startTime = System.currentTimeMillis();
    runQuery(args.query);
    System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

    return 1;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BooleanRetrievalCompressed(), args);
  }
}
