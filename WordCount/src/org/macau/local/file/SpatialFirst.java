package org.macau.local.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.local.util.FlickrData;

public class SpatialFirst {

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
        		
        		records.add(new FlickrData(id, locationID,lat, lon, timestamp, textual));
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
	
	public static void main(String[] args){
		System.out.println("The Spatial First");
		//read the data
		String dataPath ="D:\\paris.txt";
		ArrayList<FlickrData> records = readFileByLines(dataPath);
		
		int count = 0;
		for (int i = 0; i < records.size(); i++) {
			FlickrData rec1 = records.get(i);
		    for (int j = i + 1; j < records.size(); j++) {
		    	FlickrData rec2 = records.get(j);
		    	long ridA = rec1.getId();
	            long ridB = rec2.getId();
		    	if(FlickrSimilarityUtil.SpatialSimilarity(rec1, rec2)){
		    		
//		            if (ridA < ridB) {
//		                long rid = ridA;
//		                ridA = ridB;
//		                ridB = rid;
//		            }
		            count++;
//		            System.out.println(ridA + "%" + ridB);
		    	}
		    	
		    }
		    System.out.println(i + ":" +count);
		}
		
		
	}
}
