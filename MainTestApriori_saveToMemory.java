

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.List;


/**
 * Example of how to use APRIORI
 *  algorithm from the source code.
 * @author Philippe Fournier-Viger (Copyright 2008)
 */
public class MainTestApriori_saveToMemory {

	public static void main(String [] arg) throws IOException{

		String input = fileToPath("Test.txt");
		String output = null;
		// Note : we here set the output file path to null
		// because we want that the algorithm save the 
		// result in memory for this example.
		
		double minsup = 0.4; // means a minsup of 2 transaction (we used a relative support)
		
		// Applying the Apriori algorithm
		String input2 = "1 2 3 \n 4 5 6 \n 1 2 \n 1 \n 1 2 3 \n 6 7 \n 8 9 \n 10 \n 12 \n 11";
		AlgoApriori apriori = new AlgoApriori();
		Itemsets result = apriori.runAlgorithm(minsup, input2, output);
	//	apriori.printStats();
		List<String> results = result.getItemsets(apriori.getDatabaseSize());
		
		for (int i =0; i<results.size();i++){
			System.out.println(results.get(i));
		}
	}
	
	public static String fileToPath(String filename) throws UnsupportedEncodingException{
		URL url = MainTestApriori_saveToMemory.class.getResource(filename);
		 return java.net.URLDecoder.decode(url.getPath(),"UTF-8");
	}
}
