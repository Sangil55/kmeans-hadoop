/* 
 * Yonghee,Lee 2018.01.30
 * 
 * class for processing kmeans cluster algorithm
 * 
 * 
 * */
package com.swinno.hadoop.cluster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import com.swinno.hadoop.common.Config;
import com.swinno.hadoop.kmeans.PresetKmean;
import com.swinno.util.math.Distance;
import com.swinno.util.math.EuclideanDistance;

public class KmeansCluster extends Cluster {

	
	public KmeansCluster(int numDimension)
	{
		super(numDimension);
	}
	
	public KmeansCluster(int fromK, int toK) 
	{
		super(fromK,toK);
	}
	
	public KmeansCluster(int fromK, int toK, int numDimension) 
	{

		super(fromK,toK,numDimension);
	}
	
	@Override
	public int getNearestCentroid(ClusterData point, ClusterCentroids centroids) {
			
		Distance distance = new EuclideanDistance();
		return distance.getNearest(point.getData(), centroids.getKcentroidList());
	}
	
	public int getSecondNearestCentroid(ClusterData point, ClusterCentroids centroids) {
		
		Distance distance = new EuclideanDistance();
		return distance.getSecondNearest(point.getData(), centroids.getKcentroidList());
	}

	@Override
	public double[] getMean(List<double[]> points, int numElements) {
		
		int count = points.size();
		double[] sum = new double[numElements];
		double[] mean = new double[numElements];
		
		for(int i=0;i<count;i++)
		{
			double[] point = points.get(i);
			
			for(int j=0;j<numElements;j++)
			{
				sum[j] = sum[j] + point[j];
			}
		}
		
		for(int i=0;i<numElements;i++)
		{
			mean[i] = sum[i]/count;
		}
		return mean;
	}
	
	public double diff(double[] point1, double[] point2)
	{
		Distance distance = new EuclideanDistance();
		return distance.getDistance(point1, point2);
	}
	
	public double getDistance(double[] point1, double[] point2)
	{
		Distance distance = new EuclideanDistance();	
		return distance.getDistance(point1, point2);		
	}
	
	public double getDistanceSquare(double[] point1, double[] point2)
	{
		Distance distance = new EuclideanDistance();
		return distance.getDistanceSquare(point1, point2);
	}
	
	public List<Double> getCentroidResult(Job job,int k, String outputDir) throws IOException
	{
		
		JobConf conf = new JobConf();		
		conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		FileSystem fs = FileSystem.get(conf);
		
        int numPos = k%(getkRangeEnd()-getkRangeStart()+1)/10;
        
        String fileIndex = "";
        for(int i=0;i<4-numPos;i++)
        {
        	fileIndex += "0";
        }
        fileIndex = fileIndex+(k%(getkRangeEnd()-getkRangeStart()+1));
        
        System.out.println(" Get result file name ------------- : "+outputDir+"/part-r-"+fileIndex);
        
        DataInputStream d = new DataInputStream(fs.open(new Path(outputDir + "/part-r-"+fileIndex)));
        BufferedReader reader = new BufferedReader(new InputStreamReader(d));
        String line = null;
        
 
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(
        		new Path("data/kmeans/clusters"+k+".data"), true)));
        
        List<String> centroidSet = new ArrayList<String>();
        List<Double> deltaSet = new ArrayList<Double>();
        
        while ((line = reader.readLine()) != null)
        {
            if(line.trim().length() > 0)
            {
            	String token[] = line.split("\\|");
            	System.out.println(" Result ------------- : "+token[0]+"|"+token[1]);
            	StringTokenizer st = new StringTokenizer(token[0]);
            	String centroid = "";
            	String centroidIndex = "C"+st.nextToken()+",";
            	String centroidValue = st.nextToken();
            	String elements[] = centroidValue.split(",");
            	
            	for(int i=0;i<getDimension();i++)
            	{           		        
            		centroid = centroid + elements[i];
//            		System.out.println(" Centroid -------------: " + centroid);
            		if(i != (getDimension()-1))
            		{
            			centroid = centroid + ",";
            		}
            	}
                bw.append(centroidIndex+centroid);
                bw.append("\n");
                centroidSet.add(line);
                deltaSet.add(Double.parseDouble(token[1]));
                
            }
            
        }
        
        reader.close();
        bw.close();
        
		return deltaSet;
	}
	
	public double getSSEValue(int k, String outputDir) throws IOException
	{
		
		JobConf conf = new JobConf();		
		conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		FileSystem fs = FileSystem.get(conf);
		
        int numPos = k%(getkRangeEnd()-getkRangeStart()+1)/10;
        
        String fileIndex = "";
        for(int i=0;i<4-numPos;i++)
        {
        	fileIndex += "0";
        }
        fileIndex = fileIndex+(k%(getkRangeEnd()-getkRangeStart()+1));
        
        
        DataInputStream d = new DataInputStream(fs.open(new Path(outputDir + "/part-r-"+fileIndex)));
        BufferedReader reader = new BufferedReader(new InputStreamReader(d));
        String line = null;
        
        double clusterMeanSum = 0.0;
        
        while ((line = reader.readLine()) != null)
        {
            if(line.trim().length() > 0)
            {
            	String token[] = line.split("\\|");           
                clusterMeanSum = clusterMeanSum + Double.parseDouble(token[2]);
            }
            
        }
        
        System.out.println(" SSE of Clusters -------------------: K"+k+" : " + clusterMeanSum);
        
		return clusterMeanSum;
	}
	
	public double getSSE(int k, String inputDir) throws IOException
	{
		FileManager fManager = new FileManager();		
//		BufferedReader reader = fManager.getResultData(k, getkRangeStart(), getkRangeEnd(), outputDir);
		
		PresetKmean preset = new PresetKmean();
		try
		{
			preset.setConfFromConfFile();
			
		}
		catch(Exception e)
		{
//			System.out.println(" Config file read error! ------------------");
		}
		
		Config kmeansConf = Config.getInstance();
		
		String clusterDataPath = kmeansConf.CentroidDir;	
		JobConf conf = new JobConf();		
		conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		FileSystem fs = FileSystem.get(conf);
		
		DataInputStream d = new DataInputStream(fs.open(new Path(clusterDataPath + "clusters"+k+".data")));
        BufferedReader reader = new BufferedReader(new InputStreamReader(d));
        String line = null;
		
        int numElements = getDimension();
 		List<double[]> pointData = new ArrayList<double[]>();
		
		List<double[]> centroids = new ArrayList<double[]>();
		
		double sse = 0;
		
		while((line = reader.readLine()) != null)
		{
			 if(line.trim().length() > 0)
	         {
				 double [] centroid = new double[numElements];
				 String tokens[] = line.split(",");
				
//			     System.out.println("centroid string --------: " + tokens[1] +","+ tokens[2]);
				 
				 for(int i=0;i<numElements;i++)
				 {           		        
					 centroid[i] = Double.parseDouble(tokens[i+1]);
//					 System.out.println("getSSE getting centroid? --------: " + centroid[i]);
				 }
				 centroids.add(centroid);
	         }    
	           
		}
      	
//		for(double[] cent : centroids)
//		{
//			System.out.println(k+"'s centroid for SSE calculation-------: " + cent[0] + cent[1]);
//		}
		
		pointData = fManager.getInputData(inputDir,getDimension());
		
		for(double[] point:pointData)
		{
			Distance distance = new EuclideanDistance();
			Map<Double,double[]> center = distance.getNearestCenter(point, centroids);
			
			List<Double> dist = new ArrayList<Double>(center.keySet());
			
			double[] tempCent = center.get(dist.get(0));
			System.out.println("selected centroid each point-----: "+"|"+k+"|"+ point[0]+"|"+point[1]+"|"+tempCent[0]+"|"+tempCent[1] +"|"+dist.get(0));
			
			sse = sse + Math.pow((double)dist.get(0), 2);
		}
		
		System.out.println(k+"'s SSE -------------------: "+sse);
		
		return sse;
	}
	

	public int findOptimalK(Map<Integer,Double> clusterMeanSet)
	{
		int optimalK = getkRangeEnd();
		double[] deltaK = new double[getkRangeEnd()-getkRangeStart()];
		double deltaRange = clusterMeanSet.get(getkRangeStart())-clusterMeanSet.get(getkRangeEnd());
		double term = deltaRange/(double)clusterMeanSet.size();
				
		
		System.out.println("delta range ----------------: "+term);
		for(int i=0;i<(getkRangeEnd()-getkRangeStart());i++)
		{
			System.out.println("SSE of "+ (getkRangeStart()+i) +"------------: "+ BigDecimal.valueOf(clusterMeanSet.get(i+getkRangeStart())).toString());
			deltaK[i] = (clusterMeanSet.get(i+getkRangeStart())-clusterMeanSet.get(i+getkRangeStart()+1))/term;
			System.out.println(" Elbow method delta value ---------------:"+(getkRangeStart()+i)+" to " +(getkRangeStart()+i+1) 
					+" : "+ deltaK[i]);
		}
		
		for(int j=0;j<deltaK.length; j++)
		{
			System.out.println(" Elbow method ceta value ----------------:" + (getkRangeStart()+j)+" : "+ Math.atan(deltaK[j]) );
			if(Math.atan(deltaK[j]) < Math.PI/90)
			{
				optimalK = getkRangeStart()+j;
				break;
			}
		}
		
		for(int i=0;i<(getkRangeEnd()-getkRangeStart());i++)
		{
			System.out.println("delta range ----------------:(silhouete start) "+term);
		}
		return optimalK;
	}
	
	public Map<Integer,Integer> checkEndOfKclusterJob(String endJobPath) throws IOException, InterruptedException
	{
		JobConf conf = new JobConf();
		conf.setBoolean("fs.hdfs.impl.disable.cache",true);
		Map<Integer,Integer> result = new HashMap<Integer,Integer>();
		FileSystem fs = FileSystem.get(conf);
		

		DataInputStream d = new DataInputStream(fs.open(new Path(endJobPath)));
		BufferedReader reader = new BufferedReader(new InputStreamReader(d));
		String line = null;
		
		while ((line = reader.readLine()) != null)
		{
			String[] tokens = line.split("-");			
			result.put(Integer.parseInt(tokens[1].trim()),1);
		}
		
		return result;
	}

}
