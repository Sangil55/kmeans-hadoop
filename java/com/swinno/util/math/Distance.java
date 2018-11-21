/* 
 * Yonghee,Lee 2018.01.30
 * 
 * interace Class To calculate distance of points
 * 
 * 
 * */
package com.swinno.util.math;

import java.util.List;
import java.util.Map;

public interface Distance {
	
	public static final double MAX_DISTANCE = Double.MAX_VALUE;
	public static final double MIN_DISTANCE = Double.MIN_VALUE;
	
	public double getDistance(double[] point1, double[] point2);
	public double getDistance(Double[] point1, Double[] point2);
	public double getDistanceSquare(double[] point1, double[] point2);
	
	/**
	 * @author Yonghee,Lee
	 * @brief get sum of distance that one point from some points 
	 * @param point : One of point
	 * @param points : Some points
	 * @return sum of distance that one point from some points
	 */
	public default double getSumOfDistances(double[] point, List<double[]> points) {
		
		double sumOfDistance = MIN_DISTANCE;
		
		for(double[] point2 : points) {
			
			sumOfDistance = sumOfDistance + getDistance(point, point2);
		}
		
		return sumOfDistance;
	}

	/**
	 * @author Yonghee,Lee
	 * @brief get nearest point from one data
	 * @param point : One of point
	 * @param points : Some points
	 * @return index of nearest point included a list
	 */
	public default int getNearest(double[] point, List<double[]> points) {
		
		double minDistance = MAX_DISTANCE;
		int index=0;
		int minIndex=0;
		
		for(double[] point2 : points) {
			double tempDistance = getDistance(point,point2);
			if(minDistance > tempDistance) {
				minDistance = tempDistance;
				minIndex = index;
			}
			index++;
		}
		
		return minIndex;
	}
	public default int getSecondNearest(double[] point, List<double[]> points) {
		
		double minDistance = MAX_DISTANCE;
		double minSecondDistance = MAX_DISTANCE;
		int index=0;
		int minIndex=0,minSecondIndex=0;
		
		for(double[] point2 : points) {
			double tempDistance = getDistance(point,point2);
			if(minDistance > tempDistance) {
				minSecondDistance = minDistance;
				minSecondIndex = minIndex;
				minDistance = tempDistance;
				minIndex = index;
			}
			else if(minSecondDistance > tempDistance)
			{
				minSecondDistance = tempDistance;
				minSecondIndex = index;
			}
			index++;
		}
		
		return minSecondIndex;
	}
	
	public Map<Double,double[]> getNearestCenter(double[] point, List<double[]> points);
}
