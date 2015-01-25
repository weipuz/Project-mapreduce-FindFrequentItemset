package project;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.*;

public class SplitFile {
	public static void main(String[] args) {
		String input_file_name = args[0];
		int chunks_number = Integer.parseInt(args[1]);
		String output_filename = args[2];
		int chunks_size;
		int line = 0;
		ArrayList<String> final_list = new ArrayList<String>();
		File file = new File(input_file_name);
        BufferedReader reader = null;
        try {
            
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            //ArrayList<String> final_list = new ArrayList<String>();
            
           
            while ((tempString = reader.readLine()) != null) {
                
            	final_list.add(tempString);
                //System.out.println("line " + line + ": " + tempString);
                line++;
            }
            //System.out.print(line);
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        chunks_size = line/chunks_number;
       // String output_filename = "/grad/3/weipuz/output/output.dat";
        for (int i=0; i<chunks_number; i++){
        	
    		try {
            	BufferedWriter out = new BufferedWriter(new FileWriter(output_filename + Integer.toString(i)));
            	for (int j=0; j<chunks_size; j++){
            		out.write(final_list.get(j+chunks_size*i));
            		out.newLine();
                	if(j==chunks_size-1){
                		out.close();
                	}
            	}
            	
            	
            } catch (IOException e) {
            	e.printStackTrace();
            }
        	
        	
        }
        
        
        
		//System.out.println(input_file_name);
	}
}
