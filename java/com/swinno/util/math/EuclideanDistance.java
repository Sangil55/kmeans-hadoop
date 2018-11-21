/* 
 * Yonghee,Lee 2018.01.30
 * 
 * Implementation class To calculate Euclidean distance of points
 * 
 * 
 * */
package com.swinno.util.math;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EuclideanDistance implements Distance {

	public double getDistance(double[] point1, double[] point2) {
		
		return Math.sqrt(getDistanceSquare(point1,point2));
	}

	@Override
	public double getDistanceSquare(double[] point1, double[] point2) {
		
		double distanceSquare = MIN_DISTANCE;
		
		for(int i=0;i< point1.length;i++) {
			
			distanceSquare = distanceSquare+Math.pow(point1[i] - point2[i],2);
			
		}
		
		return distanceSquare;		
	}
	
	public double getDistance(Double[] point1, Double[] point2) {
		
		double distance = MIN_DISTANCE;
		
		for(int i=0;i< point1.length;i++) {
			
			distance = distance + Math.pow(point1[i] - point2[i],2);
		}
		
		return Math.sqrt(distance);
	}

	@Override
	public Map<Double, double[]> getNearestCenter(double[] point, List<double[]> points) {
		
		Map<Double,double[]> result = new HashMap<Double,double[]>();
		double minDistance = MAX_DISTANCE;
		int index=0;
		int minIndex=0;
		
		
		
		for(double[] point2 : points) 
		{
			double tempDistance = getDistance(point,point2);
			if(minDistance > tempDistance) 
			{
				minDistance = tempDistance;
				minIndex = index;
			}
			index++;
		}
		
		result.put(minDistance, points.get(minIndex));
		return result;
	}

}
