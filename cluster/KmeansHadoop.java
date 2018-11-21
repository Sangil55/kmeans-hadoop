/* 
 * Yonghee,Lee 2018.01.30
 * 
 * class for processing kmeans cluster with hadoop
 * 
 * 
 * */
package com.swinno.hadoop.cluster;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.swinno.hadoop.common.Config;
import com.swinno.hadoop.cluster.ClusterCentroids;
import com.swinno.hadoop.kmeans.PresetKmean;
import com.swinno.util.math.Distance;
import com.swinno.util.math.EuclideanDistance;

/**
 * 
 * @author Yonghee,Lee
 * @brief process kmeans algorithm with hadoop
 *
 */
public class KmeansHadoop {
	

	private static DecimalFormat df = new DecimalFormat(".00");
	
	/**
	 * 
	 * @author Yonghee,Lee
	 * @brief Mapper class for kmeans algorithm processing
	 *
	 */
	public static class KmMapper extends Mapper<Object, Text, IntWritable, DoubleArrayWritable> {
		
		private static String endJobPath = "data/kmeans/endjob.data";
		private static String clusterDataPath = "data/kmeans/";
		private ClusterCentroids centroids; 
		private Map<Integer,ClusterCentroids> centroidsHash = new HashMap<Integer,ClusterCentroids>();
		private static int fromK;
		private static int toK;
		private static int numElements;
		
		
		/**
		 * @brief get K(range)'s centroids from hadoop file system, after that, change old cluster data file name
		 *  	   if any K's job is done, skip getting centroids 
		 */
         @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            
        	PresetKmean preset = new PresetKmean();
    		try
    		{
    			preset.setConfFromConfFile();
    			
    		}
    		catch(Exception e)
    		{
//    			System.out.println(" Config file read error! ------------------");
    		}
    		Config kmeansConf = Config.getInstance();
    		endJobPath = kmeansConf.CentroidDir  + "endjob.data";
    		clusterDataPath = kmeansConf.CentroidDir;	
    		fromK = kmeansConf.getValueInt("K_RangeStart");
    		toK = kmeansConf.getValueInt("K_RangeEnd");
    		numElements = kmeansConf.getValueInt("Dimension");
    		
    		System.out.println("Config variables : fromK:"+fromK+" toK:"+toK+" numElements:"+numElements);
    		Configuration fileConf = context.getConfiguration();
    		fileConf.setBoolean("fs.hdfs.impl.disable.cache", true);
        	FileSystem fs = FileSystem.get(fileConf);
        	
            int kVal = fromK;
            
            Map<Integer,Integer> endOfKclusterJob = new HashMap<Integer,Integer>();
            KmeansCluster kmeans = new KmeansCluster(fromK, toK, numElements);
            try
            {
            	endOfKclusterJob = kmeans.checkEndOfKclusterJob(endJobPath);
            }
            catch(Exception e)
            {
            	
            }
            /*
             *  looping for each K job processing
             */
        	while(kVal<=toK)
        	{

        		
        		DataInputStream d = new DataInputStream(fs.open(new Path(clusterDataPath+"clusters"+kVal+".data")));
        		BufferedReader reader = new BufferedReader(new InputStreamReader(d));
        		String line = null;
            
        		try {
        			centroids = new ClusterCentroids(kVal);
				} catch (Exception e) {
					
					System.out.println("------- bad k value!---------");
				}
        		
        		while ((line = reader.readLine()) != null){
        			double[] elements = new double[numElements];
        			String[] tokens = line.split(",");
        			for(int i=0;i<numElements;i++)
        			{
        				elements[i] = Double.parseDouble(tokens[i+1].trim());
        			}
        			centroids.addCentroid(elements);        
        		}
         		reader.close();
         		centroidsHash.put(kVal, centroids);		
        		
         		if(!endOfKclusterJob.containsKey(kVal))
        		{
         			fs.rename(new Path(clusterDataPath+"clusters"+kVal+".data"), 
        					new Path(clusterDataPath+"clusters"+kVal+".data"+System.currentTimeMillis()));
        		}
         		
        		kVal++;
        	}
       
            super.setup(context);
        }
        /**
         * @brief mapping key and value by calculating minimum distance from a data to centroids (min K to max K)
         */
        public void  map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            //read clusters
            //assign users to clusters 
        	StringTokenizer itr = new StringTokenizer(value.toString());
        	
            while(itr.hasMoreTokens())
            {
            	double dataElements[] = new double[numElements];
            	int index = 0;
            	
            	while(index < numElements) 
            	{
            		dataElements[index] = Double.parseDouble(itr.nextToken());
            		index++;
            	}
            	for(int i=0;i<=(toK-fromK);i++)
            	{
            		KmeansCluster kmeans = new KmeansCluster(fromK, toK, numElements);
            		ClusterData data = new ClusterData(dataElements);
            		ClusterCentroids tempCentroids = centroidsHash.get(fromK+i);
            		
            		//set key
            		int centroidIndex = kmeans.getNearestCentroid(data, tempCentroids);
            		
            		IntWritable kmKey = new IntWritable((fromK+i)*1000 + (centroidIndex+1));
            		
            		//set value
            		DoubleArrayWritable resultValue = new DoubleArrayWritable();
                    DoubleWritable[] point = new DoubleWritable[numElements*2];
            		double[] outputValue = new double[numElements*2];
            		double[] nearestCentroid = tempCentroids.getCentroid(centroidIndex);
            		System.arraycopy(data.getData(), 0, outputValue, 0, numElements);
            		System.arraycopy(nearestCentroid, 0, outputValue, numElements, numElements);
            		
            		for(int j=0;j<numElements*2;j++)
            		{
            			point[j] = new DoubleWritable();
            			point[j].set(outputValue[j]);
            		}            		
            		resultValue = new DoubleArrayWritable(point);
            			
            		context.write(kmKey, resultValue);
            	}
            }
       }
        
	}

	public static class KmReducer extends Reducer<IntWritable, DoubleArrayWritable, Text, Text> {
		
		private static int fromK;
		private static int toK;
		private static int numElements;
		private static String endJobPath;
	
		
		public void reduce(IntWritable key, Iterable<DoubleArrayWritable> values, Context context) throws IOException,InterruptedException
		{
    
	       	PresetKmean preset = new PresetKmean();
	    	try
	    	{
	    		preset.setConfFromConfFile();
	    			
	    	}
	    	catch(Exception e)
	    	{
//	    		System.out.println(" Config file read error! ------------------");
	    	}
	    	Config kmeansConf = Config.getInstance();
	    	
	    	fromK = kmeansConf.K_RangeStart;
	    	toK = kmeansConf.K_RangeEnd;
	    	numElements = kmeansConf.getValueInt("Dimension");
	    	endJobPath = kmeansConf.CentroidDir + "endjob.data";
	    	
			List<double[]> points = new ArrayList<double[]>();
			double[] oldCentroid = new double[numElements];
			boolean isFirst = true;
			
			for(DoubleArrayWritable value: values)
			{
				double[] point = new double[numElements];
				
				for(int i=0;i<numElements;i++)
				{
					point[i]=((DoubleWritable) value.get()[i]).get();
					if(isFirst)
					{
						oldCentroid[i] = ((DoubleWritable) value.get()[i+numElements]).get();
					}
				}
				isFirst = false;
				
				points.add(point);
			}
			
			String centroidStr = "";
			Map<Integer,Integer> endOfKclusterJob = new HashMap<Integer,Integer>();
			KmeansCluster kmeans = new KmeansCluster(fromK, toK, numElements);
			
			try
			{
				endOfKclusterJob = kmeans.checkEndOfKclusterJob(endJobPath);
			}
			catch(Exception e)
			{
//				System.out.println("----------- endjob file didn't exist! --------------");
			}
			
			int keyVal = key.get()/1000;
			
			KmeansCluster kmean = new KmeansCluster(numElements);				
			double[] newCentroid = null;			
			double delta = 0.0;
//			Distance distance = new EuclideanDistance();
//			
//			double sse = 0.0;
			
			if(!endOfKclusterJob.containsKey(keyVal))
			{
				//get centroid Delta
				newCentroid = kmean.getMean(points, numElements);	
				delta = kmean.diff(oldCentroid,newCentroid);
			}
			else
			{
				newCentroid = new double[numElements];
				System.arraycopy(oldCentroid, 0, newCentroid, 0, numElements);
			}
			
//			for(int i=0;i<points.size();i++)
//			{
//				sse = sse + distance.getDistanceSquare(points.get(i), newCentroid);
//			}
			
			StringBuffer sb = new StringBuffer();
			for(int i=0;i<newCentroid.length;i++)
			{
				sb.append(df.format(newCentroid[i]));
				if(i != newCentroid.length-1)
					sb.append(",");
				
				if(i == newCentroid.length-1)
				{
//					sb.append("|"+delta + "|" +sse);
					sb.append("|"+delta);
				}
			}
			
			centroidStr = sb.toString();
			//print them out
			context.write(new Text((key.get()/1000) + "-" + (key.get()%1000)), new Text(centroidStr));
//			sse = 0.0;
    
		}

	}
	
	public static void main(String[] args) throws Exception {


        PresetKmean preset = new PresetKmean();
        
 	    preset.init();
     //   preset.setConfFromConfFile();
        Config conf = Config.getInstance();
//        System.out.println("K_RangeStart: "+conf.K_RangeStart);
//        System.out.println("Dimension: "+conf.Dimension);
//        System.out.println("InputDir: " + conf.InputDir);
        

        CentroidPicker picker = new CentroidPicker(conf.InputDir, conf.K_RangeStart, conf.K_RangeEnd, conf.Dimension);
        picker.startGenerate();
//        picker.generateCentroids(conf.K_RangeStart, conf.K_RangeEnd, conf.Dimension);

		JobHandler jobHandler = new JobHandler("KmeansHadoop",conf.InputDir, conf.OutputDir,conf.MaxItr, conf.K_RangeStart, 
				conf.K_RangeEnd, conf.Dimension, conf.ConvDelta);
		
		jobHandler.iterateJob();
		
		//Test code
//		KmeansCluster kcluster = new KmeansCluster(conf.K_RangeStart, conf.K_RangeEnd, conf.Dimension);
//		for(int i=conf.K_RangeStart;i<(conf.K_RangeEnd+1);i++)
//		{	
//			kcluster.getSSE(i, conf.InputDir);
//		}
	}
}
