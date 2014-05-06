package org.macau.local.analysis.time;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.local.file.ReadFlickrData;
import org.macau.local.util.FlickrData;
import org.macau.local.util.FlickrDataLocalUtil;

public class ThreeFeatureJoin {

	public static void main(String[] args) throws IOException{
		
		List<String> orderList = new ArrayList<String>();
		
		/*
		 * 1: Temporal
		 * 2: Spatial
		 * 3: Textual
		 * 
		 */
		
		for(int i = 1; i <= 3;i++){
			for(int j =1 ; j <= 3;j++){
				for(int k= 1; k <= 3;k++){
					if(i != j && j != k && i != k){
						orderList.add(i + ";" + j + ";" + k);
					}
				}
			}
		}
		
		for(String str : orderList){
			
			int[] order = new int[3];
			for(int i = 0; i < str.split(";").length;i++){
				order[i] = Integer.parseInt(str.split(";")[i]);
			}
			
			System.out.println(str + ":" + FeatureTime(order));
				
		}
		System.out.println("Finished");
		
	}
	
	
	public static double FeatureTime(int[] order) throws IOException{
		
		ArrayList<FlickrData> rRecords = ReadFlickrData.readFileByLines(FlickrDataLocalUtil.rDataPath);
		ArrayList<FlickrData> sRecords = ReadFlickrData.readFileByLines(FlickrDataLocalUtil.sDataPath);
		
		int firstCount = 0;
		int SecondCount = 0;
		int ThirdCount = 0;
		Long startTime = System.currentTimeMillis();
		
		FileWriter writer = new FileWriter(FlickrDataLocalUtil.resultPath);
		
		for (int i = 0; i < FlickrDataLocalUtil.CompareCount; i++) {
			
			FlickrData value1 = rRecords.get(i);
			
		    for (int j = 0; j < FlickrDataLocalUtil.CompareCount; j++) {
		    	
		    	FlickrData value2 = sRecords.get(j);
		    	
		    	int condition = 0;
		    	for(int m = 0; m < 3; m++){
		    		
		    		if(order[m] == 1){
		    			
		    			if(FlickrSimilarityUtil.TemporalSimilarity(value1, value2)){
				    		firstCount++;
				    		condition++;
		    			}else{
		    				break;
		    			}
		    			
		    		}else if(order[m] == 2){
		    			
		    			if(FlickrSimilarityUtil.SpatialSimilarity(value1, value2)){
				    		
			    			SecondCount++;
			    			condition++;
			    			
		    			}else{
		    				break;
		    			}
		    			
		    		}else if(order[m] == 3){
		    			
		    			if (FlickrSimilarityUtil.TextualSimilarity(value1, value2)) {
		    				
		 		        	ThirdCount++;
		 		        	condition++;
		 		        	
		 		        }else{
		 		        	break;
		 		        }
		    		}
		    	}
		    	
		    	if(condition == 3){
		    				
		    		
 		            long ridA = value1.getId();
 		            long ridB = value2.getId();
 		            if (ridA < ridB) {
 		                long rid = ridA;
 		                ridA = ridB;
 		                ridB = rid;
 		            }
 		            writer.write(ridA + "%" + ridB +"\n");
 		        }
    			
    		}
		           
		}
		
		writer.close();
//		System.out.println(firstCount);
//		System.out.println(SecondCount);
//		System.out.println(ThirdCount);
//		System.out.println("Phase One cost"+ (System.currentTimeMillis() -startTime)/ (float) 1000.0 + " seconds.");
		return (System.currentTimeMillis() -startTime)/ (float) 1000.0;
	}
}
