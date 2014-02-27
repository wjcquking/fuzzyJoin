package org.macau.local.file;

/**
 * author: wangjian
 * descrition: Get the Data from YYY about the Flickr
 * 
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class FileOperationTest {

	public final static String separator = ":";
	public static String[] readFileByLines(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        String[] records = new String[796427];
        try {
            System.out.println("Read one line");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            String[] timestamp = null;
            String[] location = null;
            String[] locationID = null;
            String[] pictureID = null;
            // One line one time until the null
            while ((tempString = reader.readLine()) != null) {
                // The line Number
                System.out.println("line " + line  + " : " + tempString.split(";").length);

                switch(line){
                //line 2: the timestamp of the picture
                case 2:
                	timestamp = tempString.split(";");
                	break;
                case 4:
                	location = tempString.split(";");
                case 5:
                	locationID = tempString.split(";");
                case 8:
                	pictureID = tempString.split(";");
                }

                line++;
            }
            
            for(int i = 0; i < pictureID.length;i++){
            	records[i] = pictureID[i] + separator+ locationID[i] + separator +location[Integer.parseInt(locationID[i])*2] + separator+ location[Integer.parseInt(locationID[i])*2+1];
            	records[i] += separator+ timestamp[i];
            }
            
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
        return records;
    }
	
	
	public static String[] readTextualByLines(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        String[] textual = new String[796427];
        try {
            System.out.println("Read one line");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            String[] timestamp = null;
            String[] location = null;
            String[] locationID = null;
            String[] pictureID = null;
            
            int locationNumber = 0;
//            int max = 0;
//            int min = 1000000;
            
            // One line one time until the null
            while ((tempString = reader.readLine()) != null) {
                // The line Number
                //System.out.println("line " + line  + " : " + tempString);
                
                if(line % 2 == 1){
                	
                	locationNumber = Integer.parseInt(tempString);
//                	if(locationNumber > max)
//                		max = locationNumber;
//                	if(locationNumber < min)
//                		min = locationNumber;
                }else if(line % 2 == 0 ){
                	
                	textual[locationNumber] = tempString;
              
                }
                
                line++;
            }
//            for(String str : textual){
//            	System.out.println(str);
//            }
//            System.out.println(max);
//            System.out.println(min);
            
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
        return textual;
    }
	
	public static void main(String[] args){
		System.out.println("Get The file of Paris");
		String path = "C:/Users/mb25428/Dropbox/06-hadoop/01-Data/Flickr/InitData/InitData/ParisDataProBuf.data";
		String[] records = readFileByLines(path);
		
		String textualPath = "C:/Users/mb25428/Dropbox/06-hadoop/01-Data/Flickr/Tag/ParisRawPhtToTags.txt";
		String[] textual = readTextualByLines(textualPath);
		
		System.out.println(records.length);
		System.out.println(textual.length);
		
		String outputPath ="D:\\paris.txt";
		try
		{

			FileWriter writer = new FileWriter(outputPath);
			
			for(int i = 0; i < records.length;i++){
				writer.write(records[i] +separator + textual[i] +"\n");;
			}
			
			//close the write
			writer.close();
		}
		catch(IOException iox)
		{
		System.out.println("Problemwriting" + outputPath);
		}
	}
}
