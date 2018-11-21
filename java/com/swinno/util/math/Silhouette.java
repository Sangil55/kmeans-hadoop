package com.swinno.util.math;

public class Silhouette {
	public static final double MIN_DISTANCE = Double.MIN_VALUE;
	
	
	public double getsilebyMean(double[] a, double[] b, double[] c) {
		
		double dist1 = Math.sqrt(getDistanceSquare(a,b));
		double dist2 = Math.sqrt(getDistanceSquare(a,c));
	//	System.out.println(dist1 + " " + dist2 + " " + (dist2-dist1) / Math.max(dist1, dist2));
		return (dist2-dist1) / Math.max(dist1, dist2);
	}
	
	public double getDistanceSquare(double[] point1, double[] point2) {
		
		double distanceSquare = MIN_DISTANCE;
		
		for(int i=0;i< point1.length;i++) {
			
			distanceSquare = distanceSquare+Math.pow(point1[i] - point2[i],2);
			
		}
		
		return distanceSquare;		
	}
}
