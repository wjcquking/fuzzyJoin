package org.macau.local.sample;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.spatial.ZOrderValue;
import org.macau.local.file.ReadFlickrData;
import org.macau.local.util.FlickrData;
import org.macau.local.util.FlickrDataLocalUtil;

/**
 * 
 * @author mb25428
 * this idea comes from the paper "On random Sampling over join"
 */

public class SpatialSample {
	
	/**
	 * Computer the join R1 and R2
	 * sample the join result Using the black-box U1 or U2
	 * @throws IOException 
	 */
	public static void spatialNaiveSample(ArrayList<FlickrData> rRecords,ArrayList<FlickrData> sRecords,double probability) throws IOException{
		
		FileWriter writer = new FileWriter(FlickrDataLocalUtil.resultPath);
		
		for (int i = 0; i < rRecords.size(); i++){
			
			FlickrData value1 = rRecords.get(i);
			
		    for (int j = 0; j < sRecords.size(); j++){
		    	
		    	FlickrData value2 = sRecords.get(j);
		    	
		    	if(FlickrSimilarityUtil.SpatialSimilarity(value1, value2)){
		    		
		    		Random random = new Random();
		    		
		    		if(random.nextDouble() < probability){
		    			
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
		    
		}
		writer.close();
	}
	
	public static void spatialOlkenSample(ArrayList<FlickrData> rRecords, int r,Map<Integer,Integer> accountMap){
		
		int max = 0;
		Iterator iter = accountMap.entrySet().iterator();
		while(iter.hasNext()){
			Map.Entry<Integer, Integer> entry = (Map.Entry<Integer, Integer>)iter.next();
			
			if(entry.getValue() > max){
				max = entry.getValue();
			}
		}
		
		for(int i = 0;i < r;i++){
			
			boolean accept = false;
			while(accept == false){
				Random random = new Random();
				FlickrData rFlickrData = rRecords.get(random.nextInt(rRecords.size()));
				int latTile =(int)(rFlickrData.getLat()/Math.sqrt(FlickrSimilarityUtil.DISTANCE_THRESHOLD));
                int lonTile = (int)(rFlickrData.getLon()/Math.sqrt(FlickrSimilarityUtil.DISTANCE_THRESHOLD));
				int zOrder = ZOrderValue.parseToZOrder(latTile, lonTile);
				double p = accountMap.get(zOrder) / (double)max;
				
				Random acceptRandom = new Random();
				
				if(acceptRandom.nextDouble() < p){
					
					FlickrData sFlickrData = new FlickrData();
					
					accept = true;
					System.out.println(rFlickrData + "%" + sFlickrData);
					
				}
			}
		}
	}
	
	public static void spatialStreamSample(){
		
		//1. first get r sample data from R Using WR2
		SpatialBlackBoxWR2.weightedBlackBoxWR2ofSpatial(FlickrDataLocalUtil.rDataPath,4000,SpatialBlackBoxWR2.getSpatialWeightedData(FlickrDataLocalUtil.sDataPath));
		
		//2. Read the data from R
		
	}
	
	public static void spatialGropuSample(int r){
			FlickrData[] reservoirArray = SpatialBlackBoxWR2.weightedBlackBoxWR2ofSpatial(FlickrDataLocalUtil.rDataPath,r,SpatialBlackBoxWR2.getSpatialWeightedData(FlickrDataLocalUtil.sDataPath));
			
			Map<FlickrData,List<FlickrData>> sampleResult = new HashMap<FlickrData,List<FlickrData>>();
			
			
			for(FlickrData fd : reservoirArray){
				List<FlickrData> list = new ArrayList<FlickrData>();
				sampleResult.put(fd, list);
			}
			
			File file = new File(FlickrDataLocalUtil.sDataPath);
			
	        BufferedReader reader = null;
			try {
	            reader = new BufferedReader(new FileReader(file));
	            String tempString = null;
	            int line = 1;
	            
	            // read the data One line one time until the null
	            while ((tempString = reader.readLine()) != null) {

	            	FlickrData sFD = new FlickrData(FlickrSimilarityUtil.getFlickrDataFromString(tempString.toString()));

	            	System.out.println(line);
	        		
	        		
	        			
        			for(FlickrData fd : reservoirArray){
        				
        				if(FlickrSimilarityUtil.SpatialSimilarity(sFD, fd)){
        					sampleResult.get(fd).add(sFD);
        				}
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
			
			Iterator iter = sampleResult.entrySet().iterator();
			
			int i = 0;
			while(iter.hasNext()){
				Map.Entry<FlickrData,List<FlickrData>> entry = (Map.Entry<FlickrData,List<FlickrData>>)iter.next();
				System.out.println(i++ + "   " + entry.getKey() +"   "+ BlackBoxU2ofSpatial(1,entry.getValue()));
			}
	}
	
	/**
	 * 
	 * @param r
	 * @param list
	 * @return A FlickrData
	 */
	public static FlickrData BlackBoxU2ofSpatial(int r,List<FlickrData> list){
		
		int N = 0;
		int i = 0;
		for(FlickrData fd: list){
			N += 1;
			Random random = new Random();
			
			if(random.nextDouble() <= 1D/N){
				i = N-1;
			}
		}
		if(list.size() > 0){
			return list.get(i);
		}else{
			return new FlickrData();
		}
	}
	
	public static void spatialFrequencyPartitionSample(int r, int t){
		Map<Integer,Integer> weightMap = SpatialBlackBoxWR2.getSpatialWeightedData(FlickrDataLocalUtil.sDataPath);
		
		Iterator iter = weightMap.entrySet().iterator();
		while(iter.hasNext()){
			Map.Entry<Integer,Integer> entry = (Map.Entry<Integer,Integer>)iter.next();
			if(entry.getValue() > t){
				
			}
		}
	}
	
	public static void SpatialCountSample(){
		
	}
	public static void main(String[] args) throws IOException{
		
		spatialGropuSample(4000);
	}
}
