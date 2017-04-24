import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
public class parse {
	
    
    public void load()
{

        try 
        {      	FileWriter fw;
        fw = new FileWriter("/home/ubuntu/project/BigDataTeam/classify/data/analyze/toanalyze.txt", true);
        BufferedWriter bw = new BufferedWriter(fw);
        PrintWriter out = new PrintWriter(bw);
       
            FileReader fr = new FileReader("/home/ubuntu/project/BigDataTeam/classify/data/classify/classified.tsv");
            BufferedReader in = new BufferedReader(fr);
            String str1;
            String str;
            String str2;
            int count=0;
            while ((str1 = in.readLine()) != null) 
            {
            	for(;count<2;count++){
            		str1=in.readLine();
            	}
            	if(count>=2){
            		str=str1;
            		str2=in.readLine();
            		String[] t= str.split("\\s+");
            		String[] x = str2.split("\\s+");
           
            		out.print(x[8]+"," + t[1]+",");
            	
                	
            		for(int i=2;i<t.length;i++){
                	
            			out.print(t[i]+" ");}
            		out.println();
            		}
            }
            in.close();
            out.close();
        } 
        catch (IOException e) 
        {
            e.printStackTrace();
        } 	
}
public static void main (String[] args){
	parse p = new parse();
	p.load();
}


}
