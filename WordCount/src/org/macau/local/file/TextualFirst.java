package org.macau.local.file;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.macau.flickr.temporal.TemporalUtil;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.join.phase2.ValueSelfJoin;
import org.macau.local.util.FlickrData;
import org.macau.local.util.FlickrDataLocalUtil;
import org.macau.paper.SimilarityFiltersFactory;
import org.macau.similarity.PartialIntersect;
import org.macau.similarity.SimilarityMetric;
import org.macau.similarity.SimilarityFilters;
import org.macau.token.TokenSimilarity;

public class TextualFirst {
	
	public static double getTokenSimilarity(String iToken,String jToken){
		//System.out.println("iToken:" + iToken + "jToken:" + jToken);
		List<String> itext = new ArrayList<String>(Arrays.asList(iToken.split(";")));
		List<String> jtext = new ArrayList<String>(Arrays.asList(jToken.split(";")));
		
		int i_num = itext.size();
		int j_num = jtext.size();
//		System.out.println(i_num + " " + j_num);
		jtext.retainAll(itext);
		int numOfIntersection = jtext.size();
		
		return (double)numOfIntersection/(double)(i_num+j_num-numOfIntersection);
	}
	public static void main(String[] args){
		
		String similarityName = "jaccard";
		float similarityThreshold = 0.01F;
		
		SimilarityFilters similarityFilters = SimilarityFiltersFactory.getSimilarityFilters(
		        similarityName, similarityThreshold);
		
		ArrayList<FlickrData> records = ReadFlickrData.readFileByLines(FlickrDataLocalUtil.dataPath);
		Long startTime = System.currentTimeMillis();
		Map<Long,ArrayList<FlickrData>> map = new HashMap<Long,ArrayList<FlickrData>>();
	
		int count = 0;
		for (int i = 0; i < 5000; i++) {
		    FlickrData rec1 = records.get(i);
		    for (int j = i + 1; j < 5000; j++) {
		    	FlickrData rec2 = records.get(j);
		        
		        int[] tokens1 = rec1.getTokens();
		        int[] tokens2 = rec2.getTokens();
	
		
		        double similarity = getTokenSimilarity(rec1.getTextual(), rec2.getTextual());
		
		        if (similarity >= similarityThreshold) {
//		        	System.out.println(rec1.getTextual());
//		        	System.out.println(rec2.getTextual());
		            long ridA = rec1.getId();
		            long ridB = rec2.getId();
		            if (ridA < ridB) {
		                long rid = ridA;
		                ridA = ridB;
		                ridB = rid;
		            }
		            count++;
		        }
		    }
		    
		}
		System.out.println(count);
		System.out.println("Phase One cost"+ (System.currentTimeMillis() -startTime)/ (float) 1000.0 + " seconds.");
	}
}
