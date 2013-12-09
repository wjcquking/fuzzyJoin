package org.macau.flickr.knn.exact.first;

import java.util.ArrayList;
import java.util.List;

import org.macau.flickr.util.FlickrValue;

public class kNNPartition {

	private int pid;

	//the pivot location information
	private double lat;
	private double lon;
	
	
	// record the count number of this partition which can be used to analysis the load balance
	private int count;
	private double minDistance;
	private double maxDistance;
	
	private List<Double> kNNDistance;

	private List<FlickrValue> FlickrValueList;
	
	public kNNPartition(){
		
	}
	
	public kNNPartition(int count){
		this.count = count;
	}
	public kNNPartition(int pid, int count){
		this.pid = pid;
		this.count = count;
		this.minDistance = 10000;
		this.maxDistance = 0;
		this.kNNDistance = new ArrayList<Double>();
	}
	
	public kNNPartition(int pid, int count,double minDistance,double maxDistance){
		this.pid = pid;
		this.count = count;
		this.minDistance = minDistance;
		this.maxDistance = maxDistance;
		
	}
	
	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public double getLon() {
		return lon;
	}

	public void setLon(double lon) {
		this.lon = lon;
	}

	public int getPid() {
		return pid;
	}

	public void setPid(int pid) {
		this.pid = pid;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public double getMinDistance() {
		return minDistance;
	}

	public void setMinDistance(double minDistance) {
		this.minDistance = minDistance;
	}

	public double getMaxDistance() {
		return maxDistance;
	}

	public void setMaxDistance(double maxDistance) {
		this.maxDistance = maxDistance;
	}

	public List<Double> getkNNDistance() {
		return kNNDistance;
	}

	public void setkNNDistance(List<Double> kNNDistance) {
		this.kNNDistance = kNNDistance;
	}
	
}
