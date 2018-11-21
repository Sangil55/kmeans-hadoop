package com.swinno.hadoop.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author Yonghee,Lee
 * @brief class to handle centroids set
 *
 */
public class ClusterCentroids {
	
	private List<double[]> kCentroidList = new ArrayList<double[]>();
	private int k=0;
	
	public ClusterCentroids(int k) throws Exception {
		if(k == 0)
			throw new Exception();
		this.k = k;
	}
	
	public List<double[]> getKcentroidList() 
	{
		return kCentroidList;
	}
	
	public void addCentroid(double[] elements) 
	{
		this.kCentroidList.add(elements);
	}

	public double[] getCentroid(int index)
	{
		if(kCentroidList.size() <= index)
		{
			System.out.println("Error at req index :" + index + " size : " + kCentroidList.size());
			return null;
		}
		return this.kCentroidList.get(index);
	}
	
	public int getK() 
	{
		return this.k;
	}
	
	
}
