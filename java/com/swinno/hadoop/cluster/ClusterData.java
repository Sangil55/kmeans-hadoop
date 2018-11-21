package com.swinno.hadoop.cluster;

import java.io.Serializable;

public class ClusterData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private double[] data;
	private double[] centroid;
	private int currentCentroidIndex;
	private int Kvalue;
	private double distanceToCentroid;

	public ClusterData(double[] elements)
	{
		this.data = elements;
	}
	public double[] getData() 
	{
		return data;
	}

	public void setData(double[] data) 
	{
		this.data = data;
	}

	public int getKvalue() 
	{
		return Kvalue;
	}

	public void setKvalue(int Kvalue) 
	{
		this.Kvalue = Kvalue;
	}

	public int getCurrentCentroidIndex() 
	{
		return currentCentroidIndex;
	}

	public void setCurrentCentoridIndex(int currentCentroidIndex) 
	{
		this.currentCentroidIndex = currentCentroidIndex;
	}

	public double getDistanceToCentroid() {
		return distanceToCentroid;
	}
	public void setDistanceToCentroid(double distanceToCentroid) {
		this.distanceToCentroid = distanceToCentroid;
	}
	public double[] getCentroid() {
		return centroid;
	}
	public void setCentroid(double[] centroid) {
		this.centroid = centroid;
	}
		
}
