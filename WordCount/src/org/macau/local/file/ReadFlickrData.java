package org.macau.local.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.macau.local.util.FlickrData;

public class ReadFlickrData {

	public static ArrayList<FlickrData> readFileByLines(String fileName) {
		
        File file = new File(fileName);
        BufferedReader reader = null;
        ArrayList<FlickrData> records = new ArrayList<FlickrData>();
        try {
            System.out.println("Read one line");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // One line one time until the null
            while ((tempString = reader.readLine()) != null) {
                // The line Number
//                System.out.println("line " + line  + " : " + tempString.split(":").length);

                String[] flickrData = tempString.split(":");
                
                long id = Long.parseLong(flickrData[0]);
                int locationID = Integer.parseInt(flickrData[1]);
                double lat = Double.parseDouble(flickrData[2]);
                double lon = Double.parseDouble(flickrData[3]);
                
        		long timestamp = Long.parseLong(flickrData[4]);
        		
        		String textual = flickrData[5];
        		if(!textual.equals("null")){
	        		int[] tokens = new int[textual.split(";").length];
	        		
	        		for(int i = 0; i < textual.split(";").length;i++){
	        			tokens[i] = Integer.parseInt(textual.split(";")[i]);
	        		}
	        		
	        		records.add(new FlickrData(id, locationID,lat, lon, timestamp, textual,tokens));
        		}else{
        			
        			records.add(new FlickrData(id, locationID,lat, lon, timestamp, textual,new int[0]));
        		}
                line++;
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
}
