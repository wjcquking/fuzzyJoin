package org.macau.flickr.spatial.minimal;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class MiniSpatialReducer extends
	Reducer<DoubleWritable, FlickrValue, DoubleWritable, Text>{

	private final Text text = new Text();
	private final ArrayList<FlickrValue> records = new ArrayList<FlickrValue>();
	
	public void reduce(DoubleWritable key, Iterable<FlickrValue> values,
			Context context) throws IOException, InterruptedException{
		for(FlickrValue value:values){
			FlickrValue recCopy = new FlickrValue(value);
			context.write(key, new Text(value.toString()));
		    records.add(recCopy);
		}
		
	}
	
}
