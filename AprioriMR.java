package project;
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
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Reducer.Context;
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
			minSup = conf.getFloat("support",0.01f);
			
		}


		//@Override
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			
			String input = value.toString();
			String output = null;
			
			//minSup = 0.01;
			
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

		
		
		public static class TokenizerMapper2
	    extends Mapper<Object, Text, Text, IntWritable>{

	        private Text word = new Text();

	        private Configuration conf;
	        private BufferedReader fis;
	        
	        private Set<String> ItemSets = new HashSet<String>();

	        private Map<String,Integer > itemsMap = new HashMap<String,Integer>();

	        @Override
	        public void setup(Context context) throws IOException,
	        InterruptedException {
	        	
	            conf = context.getConfiguration();
	            URI[] FrqSetsURI = Job.getInstance(conf).getCacheFiles();
	            Path FrqSetsPath = new Path(FrqSetsURI[0].getPath());


	           // String tempPath = conf.get("temppath");
	            String FrqSetsFileName = FrqSetsPath.getName().toString();
	           // String itemSetsPath = tempPath+"/"+"part-r-00000";

	            FileReader fr = new FileReader(FrqSetsFileName);
	            BufferedReader br = new BufferedReader(fr);

	            String anItemSet;
	            int count = 0;

	            while ((anItemSet = br.readLine())!=null){
	            	String[] IArr = anItemSet.split(" ");
	            	String last= IArr[IArr.length-1];
	            	String[] ItemSet = anItemSet.split(last);
	                itemsMap.put(ItemSet[0], count);

	            }

	            br.close();
	        }




	        @Override
	        public void map(Object key, Text value, Context context

	                ) throws IOException, InterruptedException {

	            String input = value.toString();
	            String output = null;
	            String [] data_in = input.split("\n");
				String line=null;
	            
	         // for each line (transactions) until the end of the file
				
				Set<String> itemSet;
				Set<String> transactionSet;
	            for (Map.Entry<String, Integer> entry : itemsMap.entrySet()) { 
					itemSet = new HashSet<String>(Arrays.asList(entry.getKey().split(" ")));
					int count = 0;
					itemsMap.put(entry.getKey(), count);
					for (int j=0;j<data_in.length;j++) {           
						line=data_in[j].trim();
						transactionSet = new HashSet<String>(Arrays.asList(line.split(" ")));

						if (transactionSet.containsAll(itemSet)){
							count = itemsMap.get(entry.getKey());
							itemsMap.put(entry.getKey(), count+1);
						}
						}
					

				}
				
							
				for (Map.Entry<String, Integer> entry : itemsMap.entrySet()) { 
					context.write(new Text( entry.getKey()), new IntWritable(entry.getValue()));

				}
				
			}


	          


	    }
		
	
		public static class IntSumReducer2
		extends Reducer<Text,IntWritable,Text,IntWritable> {
			
			double supportThrld;
			private IntWritable result = new IntWritable();
			
			 @Override
		    public void setup(Context context) throws IOException, InterruptedException {
			        Configuration conf = context.getConfiguration();
			        supportThrld = conf.getFloat("support", 0.01f);
			        long totalLines = conf.getLong("TOTALNUM", 10000);
			        if(totalLines == 0)
			            throw new InterruptedException("Total number of baskets can not be zero!");

			        supportThrld = (int) (supportThrld * totalLines);
			        System.out.println("supportThrld is " + supportThrld);
			    }

			@Override
			public void reduce(Text key, Iterable<IntWritable> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      
	     if (sum > supportThrld){ 
	      result.set(sum);
	      context.write(key, result);
	      }
	      //context.write(new Text(new Integer(sum).toString()), one);
	      
		}
		}
	
	
	public static void main(String[] args) throws Exception {
	
			Configuration conf = new Configuration();

			GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
			String[] remainingArgs = optionParser.getRemainingArgs();
			if (remainingArgs.length != 5 ) {
				System.err.println("Usage: AprioriMR <in> <tmp> <out> <NumberofLinesPerRecord> <support(percentage)> ");
				System.exit(2);
			}
			
			Path inputpath = new Path(remainingArgs[0]);
			Path tmppath = new Path(remainingArgs[1]);
			Path outputpath = new Path(remainingArgs[2]);
			int NofLines =  Integer.parseInt(remainingArgs[3]);
			float support = Float.parseFloat(remainingArgs[4]);
			
			conf.set("temppath", remainingArgs[1]);
			conf.setFloat("support", support);
			conf.setInt("NofLines",NofLines);
			Job job = Job.getInstance(conf, "AprioriMR");
			job.setJarByClass(AprioriMR.class);

			job.setMapperClass(TokenizerMapper1.class);
			job.setReducerClass(IntSumReducer1.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			//List<String> otherArgs = new ArrayList<String>();
			//myinput.N =  hunks;
			job.setInputFormatClass(MyNLinesInputFormat.class);
			
			
			FileInputFormat.addInputPath(job, inputpath);
			FileOutputFormat.setOutputPath(job, tmppath);
			job.waitForCompletion(true);
			//int numberofchunks = 100;
			long totalNum = job.getCounters().findCounter(Task.Counter.MAP_INPUT_RECORDS).getValue();

            // create and execute job two
            conf.setLong("TOTALNUM", totalNum*NofLines);
            //conf.setLong("TOTALNUM", 100000);

			
			Path freqpath = new Path(remainingArgs[1]+"/"+"part-r-00000");
			Job jobtwo = Job.getInstance(conf, "AprioriMRTwo");
			jobtwo.setJarByClass(AprioriMR.class);
			jobtwo.addCacheFile(freqpath.toUri());
			jobtwo.setMapperClass(TokenizerMapper2.class);
			jobtwo.setReducerClass(IntSumReducer2.class);
			jobtwo.setOutputKeyClass(Text.class);
			jobtwo.setOutputValueClass(IntWritable.class);
			
			jobtwo.setInputFormatClass(MyNLinesInputFormat.class);
			FileInputFormat.addInputPath(jobtwo, inputpath);
			FileOutputFormat.setOutputPath(jobtwo, outputpath);
			jobtwo.waitForCompletion(true);
			
			
			
		
	
		
		
	}

}	