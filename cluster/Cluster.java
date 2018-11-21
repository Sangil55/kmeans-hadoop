/* 
 * Yonghee,Lee 2018.01.30
 * 
 * abstract class to process cluster algorithm
 * 
 * 
 * */
package com.swinno.hadoop.cluster;
import java.util.List;


public abstract class Cluster {
	
	private int kRangeStart;
	private int kRangeEnd;
	private int dimensionofVector;
	
	public Cluster(int dimensionofVector)
	{
		this.dimensionofVector = dimensionofVector;
	}
	
	public Cluster(int fromK, int toK) 
	{
		this.kRangeStart = fromK;
		this.kRangeEnd = toK;
	}
	
	public Cluster(int kRangeStart, int kRangeEnd, int dimensionofVector)
	{
		this.kRangeStart = kRangeStart;
		this.kRangeEnd = kRangeEnd;
		this.dimensionofVector = dimensionofVector;
	}
	
	public void setDemension(int num) {
		this.dimensionofVector = num;
	}
	
	public int getDimension() {
		return this.dimensionofVector;
	}
	
	public abstract int getNearestCentroid(ClusterData point, ClusterCentroids centroids);
	
	public abstract double[] getMean(List<double[]> points, int numElements);

	public int getkRangeStart() {
		return kRangeStart;
	}

	public void setkRangeStart(int kRangeStart) {
		this.kRangeStart = kRangeStart;
	}

	public int getkRangeEnd() {
		return kRangeEnd;
	}

	public void setkRangeEnd(int kRangeEnd) {
		this.kRangeEnd = kRangeEnd;
	}
	
}
