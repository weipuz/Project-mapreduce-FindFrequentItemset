import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.LineReader;



public class AprioriMR {
	private final static IntWritable one = new IntWritable(1);
	
	public static final Log LOG = LogFactory.getLog(AprioriMR.class);

	public static class TokenizerMapper1
	extends Mapper<Object, Text, Text, IntWritable>{

		static enum CountersEnum { INPUT_WORDS }
		
		/** Frequent Items */
		private List<int[]> fre_itemsets = new ArrayList<int[]>();
		
		/** number of different items in the dataset */
        private int numItems; 
        /** total number of transactions in transaFile */
        private int numTransactions; 
        /** minimum support for a frequent itemset in percentage, e.g. 0.8 */
        private double minSup; 
        
        private List<int[]> itemsets ;

        public String[] data_in;
		
		private Text word = new Text();

		private boolean caseSensitive;
		private Set<String> patternsToSkip = new HashSet<String>();

		private Configuration conf;
		private BufferedReader fis;

		//@Override
		public void setup(Context context) throws IOException,
		InterruptedException {
			conf = context.getConfiguration();
			caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
			if (conf.getBoolean("wordcount.skip.patterns", true)) {
				URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
				for (URI patternsURI : patternsURIs) {
					Path patternsPath = new Path(patternsURI.getPath());
					String patternsFileName = patternsPath.getName().toString();
					parseSkipFile(patternsFileName);
				}
			}
		}

		private void parseSkipFile(String fileName) {
			try {
				fis = new BufferedReader(new FileReader(fileName));
				String pattern = null;
				while ((pattern = fis.readLine()) != null) {
					patternsToSkip.add(pattern);
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file '"
						+ StringUtils.stringifyException(ioe));
			}
		}

		//@Override
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			
			String input = value.toString();
			String output = null;
			
			minSup = 0.01;
			
			AlgoApriori ap = new AlgoApriori();
			
			Itemsets result = ap.runAlgorithm(minSup, input, output);
			
			List<String> results = result.getItemsets(ap.getDatabaseSize());
			
			for (int i =0; i<results.size();i++){
				context.write(new Text(results.get(i)), one);
			}
		
			
			}
		
		
		
				
	}	
	
	/**
	 * Start of the Reducer code
	 * @author cloudera
	 *
	 */
		public static class IntSumReducer1
		extends Reducer<Text,IntWritable,Text,IntWritable> {
			private IntWritable result = new IntWritable();

			public void reduce(Text key, Iterable<IntWritable> values,
					Context context
					) throws IOException, InterruptedException {
				
				context.write(key, one);
			}
		}

	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		if (!(remainingArgs.length != 2 || remainingArgs.length != 4)) {
			System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "AprioriAlgo");
		job.setJarByClass(AprioriMR.class);

		job.setMapperClass(TokenizerMapper1.class);


		job.setCombinerClass(IntSumReducer1.class);
		job.setReducerClass(IntSumReducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		List<String> otherArgs = new ArrayList<String>();
	    for (int i=0; i < remainingArgs.length; ++i) {
	      if ("-skip".equals(remainingArgs[i])) {
	        job.addCacheFile(new Path(remainingArgs[++i]).toUri());
	        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
	      } else {
	        otherArgs.add(remainingArgs[i]);
	      }
	    }
	//	job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.NLineInputFormat.class);
	//	job.getConfiguration().set("mapreduce.input.lineinputformat.linespermap", "3");
		job.setInputFormatClass(MyNLinesInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}	