package org.macau.flickr.job;

import org.apache.hadoop.conf.Configuration;
import org.macau.flickr.spatial.analysis.SpatialAccount;
import org.macau.flickr.temporal.analysis.TemporalAccount;

public class FlickrSimilarityJoin {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		//FileSystem.get(conf).delete(new Path(FlickrSimilarityUtil.flickrOutputPath), true);
		Long startTime = System.currentTimeMillis();
		
//		boolean state = TemporalJoinJob.TemporalSimilarityBasicJoin(conf);
//		boolean state = TemporalAccount.TemporalAccountJob(conf);
		boolean state = SpatialAccount.spatialAccountJob(conf);
//		boolean state = SpatialSimilarityJoin.SpatialSimilarityBasicJoin(conf);
//		boolean state = TemporalSimilarityJoin.TemporalSimilarityBasicJoin(conf);
		
		
		if(state){
			System.out.println("Phase One cost"+ (System.currentTimeMillis() -startTime)/ (float) 1000.0 + " seconds.");
		}
	}
}
