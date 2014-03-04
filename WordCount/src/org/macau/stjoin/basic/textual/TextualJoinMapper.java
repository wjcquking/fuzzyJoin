package org.macau.stjoin.basic.textual;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.util.SimilarityUtil;


public class TextualJoinMapper extends
	Mapper<Object, Text, IntWritable, FlickrValue>{
		
	private final IntWritable outputKey = new IntWritable();
	
	private final FlickrValue outputValue = new FlickrValue();
	
	public static String convertDateToString(Date date){
		SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd");
		return df.format(date);
	}
	
	public static Date convertLongToDate(Long date){
		return new Date(date);
	}
	
	public static int getTagByFileName(String fileName){
		
		if(fileName.contains(FlickrSimilarityUtil.R_TAG)){
			
			return FlickrSimilarityUtil.R_tag;
			
		}else{
			return FlickrSimilarityUtil.S_tag;
		}
		
	}
	
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		InputSplit inputSplit = context.getInputSplit();
				
		//get the the file name which is used for separating the different set
		String fileName = ((FileSplit)inputSplit).getPath().getName();
				
		
		int tag = getTagByFileName(fileName);
		
		long id =Long.parseLong(value.toString().split(":")[0]);
		double lat = Double.parseDouble(value.toString().split(":")[2]);
		double lon = Double.parseDouble(value.toString().split(":")[3]);
		long timestamp = Long.parseLong(value.toString().split(":")[4]);
		
		
		
		
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTag(tag);
		outputValue.setTiles(value.toString().split(":")[5]);
		outputValue.setTimestamp(timestamp);
		
		String textual = value.toString().split(":")[5];
		
		System.out.println(!textual.equals("null"));
		if(!textual.equals("null")){
			
			String[] textualList = textual.split(";");
			
			
			//get the prefix values
			int prefixLength = SimilarityUtil.getPrefixLength(textualList.length, FlickrSimilarityUtil.TEXTUAL_THRESHOLD);
			
			for(int i = 0; i < prefixLength;i++){
				
				Integer tokenID = Integer.parseInt(textualList[i]);
				
				outputValue.setTileNumber(tokenID);
				
				outputKey.set(tokenID);
				context.write(outputKey, outputValue);
				
			}
			
		}
		

		
	}

}
