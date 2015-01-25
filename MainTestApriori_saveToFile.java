//package ca.pfv.spmf.test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;


/**
 * Example of how to use APRIORI algorithm from the source code.
 * @author Philippe Fournier-Viger (Copyright 2008)
 */
public class MainTestApriori_saveToFile {

	public static void main(String [] arg) throws IOException{

		String input = fileToPath("contextPasquier99.txt");
		String output = "/home/cloudera/workspace/Apriori/src/output.txt";  // the path for saving the frequent itemsets found
		
		double minsup = 0.2; // means a minsup of 2 transaction (we used a relative support)
		
		// Applying the Apriori algorithm
		AlgoApriori apriori = new AlgoApriori();
		Itemsets s = apriori.runAlgorithm(minsup, input, output);
		List<List<Itemset>> itemlist = s.getLevels();
		
		List frequentList = new ArrayList();
		
		for (int i = 0; i<itemlist.size();i++){
			Itemset item = (Itemset) frequentList.get(i);
			System.out.println(item.toString());
			frequentList.add(item.toString());
			
		}
		
	//	apriori.printStats();
	}
	
	public static String fileToPath(String filename) throws UnsupportedEncodingException{
		URL url = MainTestApriori_saveToFile.class.getResource(filename);
		 return java.net.URLDecoder.decode(url.getPath(),"UTF-8");
	}
}
