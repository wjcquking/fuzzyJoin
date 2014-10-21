package org.macau.flickr.job;

import org.apache.hadoop.conf.Configuration;
import org.macau.flickr.knn.exact.first.PartitionJob;
import org.macau.flickr.knn.exact.second.kNNJoinJob;
import org.macau.flickr.spatial.analysis.SpatialAccount;
import org.macau.flickr.spatial.basic.BasicSpatialRSSimilarityJoin;
import org.macau.flickr.spatial.grid.GridSpatialSimilarityJoin;
import org.macau.flickr.spatial.minimal.MiniSpatialSort;
import org.macau.flickr.spatial.minimal.MiniSpatialSample;
import org.macau.flickr.spatial.sjmr.SJMRSpatialSimilarityJoin;
import org.macau.flickr.spatial.sjmr.rs.SJMRSpatialRSSimilarityJoin;
import org.macau.flickr.temporal.analysis.TemporalAccount;
import org.macau.flickr.temporal.basic.TemporalJoinMapper;
import org.macau.stjoin.basic.temporal.TemporalJoinJob;

public class FlickrSimilarityJoin {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		//FileSystem.get(conf).delete(new Path(FlickrSimilarityUtil.flickrOutputPath), true);
		Long startTime = System.currentTimeMillis();
		System.out.println("The Start time is " + startTime);
		
//		boolean state = TemporalJoinJob.TemporalSimilarityBasicJoin(conf);
//		boolean state = TemporalAccount.TemporalAccountJob(conf);
//		boolean state = SpatialAccount.spatialAccountJob(conf);
//		boolean state = SJMRSpatialSimilarityJoin.SJMRSpatialJoin(conf);
		
//		boolean state = SpatialSimilarityJoin.SpatialSimilarityBasicJoin(conf);
//		boolean state = TemporalSimilarityJoin.TemporalSimilarityBasicJoin(conf);
		
		//the spatial RS similairty Join
//		boolean state = SJMRSpatialRSSimilarityJoin.SJMRSpatialJoin(conf);
//		boolean state = GridSpatialSimilarityJoin.GridSpatialJoin(conf);
//		boolean state = MiniSpatialSample.MiniSpatial(conf);
//		boolean state = MiniSpatialSort.MiniSpatial(conf);
		
//		boolean state = BasicSpatialRSSimilarityJoin.BasicSpatialJoin(conf);
		
		/*
		 * the kNN spatial Join
		 */
//		boolean state = PartitionJob.spatialPartitionjob(conf);
//		
		boolean state = TemporalJoinJob.TemporalSimilarityBasicJoin(conf);
//		boolean state = org.macau.stjoin.basic.spatial.GridSpatialSimilarityJoin.GridSpatialJoin(conf);
		
//		boolean state = org.macau.stjoin.basic.textual.TextualJoinJob.TextualSimilarityBasicJoin(conf);
		
		if(state){
			System.out.println("End at " + System.currentTimeMillis());
			System.out.println("Phase One cost"+ (System.currentTimeMillis() -startTime)/ (float) 1000.0 + " seconds.");
		}
		
//		boolean second = kNNJoinJob.spatialPartitionjob(conf);
//		if(second){
//			System.out.println("Phase Two cost"+ (System.currentTimeMillis() -startTime)/ (float) 1000.0 + " seconds.");
//		}
	}
}
