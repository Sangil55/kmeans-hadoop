/* 
 * Yonghee,Lee 2018.01.30
 * 
 * Class To handle Hadoop Job
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
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;		
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONArray;
import org.json.JSONObject;

import com.swinno.hadoop.cluster.KmeansHadoop.KmMapper;
import com.swinno.hadoop.cluster.KmeansHadoop.KmReducer;
import com.swinno.hadoop.cluster.KmeansHadoop;
import com.swinno.hadoop.cluster.DoubleArrayWritable;
import com.swinno.hadoop.common.Config;
import com.swinno.hadoop.kmeans.HDFSManager;

/**
 * 
 * @author Yonghee,Lee
 * @brief class for handling hadoop job. iteration, check end condition, etc.
 *
 */
public class JobHandler {
	
	private static String jobName;
	private int iterationCount = 1;
	private static String inputDir;
	private static String outputDir;
	private static int fromK;
	private static int toK;
	private static int finalindex;
	private static double converDelta;
	private static int numElements;
	
	
	public JobHandler(String jobName, String inputDir, String outputDir, int iterationCount, int fromK, int toK, int numElements, double converDelta)
	{
		this.jobName = jobName;
		this.inputDir = inputDir;
		this.outputDir = outputDir;
		this.iterationCount = iterationCount;
		JobHandler.fromK = fromK;
		JobHandler.toK = toK;
		JobHandler.numElements = numElements;
		JobHandler.converDelta = converDelta;		
		
	}
	
	public static List<List<Double>> submitJob(String jobName, String inputDir, String outputDir) throws Exception
	{
		JobConf conf = new JobConf();
		Job job = new Job(conf,jobName);
        job.setJarByClass(KmeansHadoop.class);
        job.setPartitionerClass(KmeansPartitioner.class);
        job.setMapperClass(KmMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleArrayWritable.class);
        job.setReducerClass(KmReducer.class);
        job.setNumReduceTasks(JobHandler.toK-JobHandler.fromK+1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputDir));
        //System.out.println(x);
        FileOutputFormat.setOutputPath(job, new Path(outputDir));
        job.waitForCompletion(true);
        
        
        List<List<Double>> deltaSetList = new ArrayList<List<Double>>();
        KmeansCluster kmeans = new KmeansCluster(fromK, toK, numElements);
        
        for(int i=fromK;i<(toK+1);i++)
        {
                	
        	try
        	{
//        		deltaSetList.add(getCentroidResult(job,i,outputDir));   
        		deltaSetList.add(kmeans.getCentroidResult(job,i,outputDir));        		

        	}
        	catch(Exception e)
        	{
//        		System.out.println("------------------- result to new data failed! ------------------");
        		e.printStackTrace();
        	}
        }
        
        return deltaSetList;
    }
	
	public void iterateJob() throws Exception 
	{
		
        int index = 0;
        int kIndex = 0;
        List<List<Double>> deltaSetList = null;
        Map<Integer,Integer> jobHash = new HashMap<Integer,Integer>();
        Map<Integer,Double> clusterMeanSet = new HashMap<Integer,Double>();
        KmeansCluster kmeans = new KmeansCluster(fromK, toK, numElements);
        try
        {
        	jobHash = checkEndOfKclusterJob();
        }
        catch(Exception e)
        {
        	
        }
        System.out.println("itr count = " + this.iterationCount);
		while(index < this.iterationCount)
		{
            int tempIter = index+1;
            
            Map<Integer,Integer> endJobs = readEndJob();
            
            //if all k meets Convergence Delta, end all processing   
            try 
            {
            	if(endJobs != null && endJobs.size() == (toK-fromK+1))
            		break;
            }
            catch(Exception e)
            {
//            	System.out.println("----------- read End Jobs failed! -------------");
            }
            System.out.println(" Running "+ tempIter + " iteration---------------------------------");
            
            deltaSetList = submitJob(jobName,inputDir, outputDir+"job"+index);
            
            if(deltaSetList !=null)
            	for(List<Double> deltaSet : deltaSetList)
            	{
//            		if(endJobs.containsKey(fromK+kIndex))
//            			continue;
//            	
            		double deltaSum = 0.0;
            		int count = 0;
            		for(int i=0;i<deltaSet.size();i++)
            		{
            			deltaSum += deltaSet.get(i);
            			count++;
            		}
            		double meanDelta = deltaSum/(double)count;
            		System.out.println(" Mean of Centroid Delta ----------- :K"+ (fromK+kIndex)+" : "+meanDelta);
            		if(isConvergenceDelta(meanDelta,converDelta))
            		{	
            			System.out.println(" End K Job ---------------- : "+(fromK+kIndex));
            			try
            			{
            				setEndJob(fromK+kIndex);
            			}
            			catch(Exception e)
            			{
            		
            			}
            		}
            	
            		kIndex++;
            	}
            kIndex = 0;
            index++;
		}

	
		for(int i=0;i<(toK-fromK+1);i++)
		{
			clusterMeanSet.put(fromK+i,kmeans.getSSE(fromK+i, inputDir));
		}
		
		int optK = kmeans.findOptimalK(clusterMeanSet);
		System.out.println(" Optimal K ------------------: "+optK);
		finalindex = index-1;
		
		//MakeFinalToJSON(optK);
		
		double min = Double.MAX_VALUE;
		int finalK = 0;
		for(int i= fromK; i<=toK;i++)
		{
			SilhouetteChecker sile = new SilhouetteChecker(inputDir,outputDir,i);
			double d = sile.checkbyMean();
			if(min > d)
			{
				finalK = i;
				min = d;
			}
			sile = null;
		}
		
		System.out.println(" Optimal K(sile) ------------" +finalK);
		
	}	
	
	public boolean isConvergenceDelta(double meanDelta, double converDelta)
	{
		if(meanDelta<=converDelta)
			return true;
		
		return false;
	}
	
	public void setEndJob(int k) throws IOException
	{
		
		JobConf conf = new JobConf();
		Path endJobPath = new Path("data/kmeans/endjob.data");
		conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		FileSystem fs = FileSystem.get(conf);
		List<Integer> endJobList = new ArrayList<Integer>();
		Map<Integer,Integer> endJobHash = null;
		
		endJobHash = readEndJob();
		
		if(endJobHash == null)
			endJobHash = new HashMap<Integer,Integer>();
		
		endJobHash.put(k, 1);
		
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(
					endJobPath, true)));
		
		Set<Integer> endJobK = endJobHash.keySet();
		
		endJobList = new ArrayList<Integer>(endJobK);
		for(int kVal : endJobList)
		{
			bw.append("K-"+kVal);
	        bw.append("\n");
		}
		
        bw.close();
		
	}
	
	public Map<Integer,Integer> readEndJob() throws IOException
	{
		Map<Integer,Integer> result = new HashMap<Integer,Integer>();
		
		JobConf conf = new JobConf();
		Path endJobPath = new Path("data/kmeans/endjob.data");
		conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		FileSystem fs = FileSystem.get(conf);
		DataInputStream d = null;
		BufferedReader reader = null;
		boolean fileExist = true;
		
		try
		{
			d = new DataInputStream(fs.open(endJobPath));
			reader = new BufferedReader(new InputStreamReader(d));
			
		}
		catch(Exception e)
		{
			fileExist = false;
		}
		
		if(fileExist)
		{
//			result = new HashMap<Integer,Integer>();
			String line = null;
			while ((line = reader.readLine()) != null)
			{
				String tokens[] = line.split("-");
				int kVal = Integer.parseInt(tokens[1]);
				result.put(kVal, 1);				
			}
		}
		if(reader != null)
			reader.close();
		
		return result;
	}
	
	/* check Kth job is done
	 * To-Do : extract to another class 
	 */
	public Map<Integer,Integer> checkEndOfKclusterJob() throws IOException, InterruptedException
	{
		JobConf conf = new JobConf();
		Map<Integer,Integer> result = new HashMap<Integer,Integer>();
		conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		FileSystem fs = FileSystem.get(conf);
		
		DataInputStream d = new DataInputStream(fs.open(new Path("data/kmeans/endjob.data")));
		BufferedReader reader = new BufferedReader(new InputStreamReader(d));
		String line = null;
		
		while ((line = reader.readLine()) != null)
		{
			String[] tokens = line.split("-");			
			result.put(Integer.parseInt(tokens[1]),1);
		}
		
		return result;
	}
	
	private void MakeFinalToJSON(int K)
	{
		String inputpath = Config.getInstance().InputDir+"inputdata"; 
		Configuration conf = new Configuration();
		int numElements = Config.getInstance().Dimension;
		HDFSManager hdfsmanager;
		try {
			hdfsmanager = new HDFSManager(conf ,FileSystem.get(URI.create(inputpath),conf) );
			/////////////first get centriods
			int numPos = K%(toK-fromK+1)/10;
	        
	        String fileIndex = "";
	        for(int i=0;i<4-numPos;i++)
	        {
	        	fileIndex += "0";
	        }
	        fileIndex = fileIndex+(K%(toK-fromK+1));
	
			FileSystem fs = hdfsmanager.getFileSystem();
			System.out.println(outputDir +"job"+finalindex+ "/part-r-"+fileIndex);
		    DataInputStream d = new DataInputStream(fs.open(new Path(outputDir +"job"+finalindex+ "/part-r-"+fileIndex)));
	        BufferedReader reader = new BufferedReader(new InputStreamReader(d));
	        String line = null;
	        ClusterCentroids centroids;
	        centroids = new ClusterCentroids(K);
			
			while ((line = reader.readLine()) != null){
		        String token[] = line.split("\\|");
				double[] elements = new double[numElements];
			 	StringTokenizer st = new StringTokenizer(token[0]);
			 	System.out.println(st.nextToken());
			 	String str = st.nextToken();
			 	String tokens[] = str.split(",");
				for(int i=0;i<numElements;i++)
				{
					System.out.println(tokens[i]);
					elements[i] = Double.parseDouble(tokens[i]);
				}
				centroids.addCentroid(elements);
				System.out.println(elements[0] + " , " + elements[1]);			
			}
	 		reader.close();
	 		
	 		//System.out.println(centroids.getK());
	 		////////////second get data and link with cluster 
			if(!hdfsmanager.exists(inputpath))
			{	
				System.out.println("----- There is no file on "+inputpath);
				return;
			}
			FSDataInputStream inputStream;
			inputStream = hdfsmanager.getFileSystem().open(new Path(inputpath));
			double data[][] = new double[Config.getInstance().row][Config.getInstance().col];
			//ClusterData Finaldata[] = new ClusterData[Config.getInstance().row];
			int counter[] = new int[K];
			List <Integer> []link = new List[K];
			for(int i=0;i<K;i++) 	
				link[i] = new ArrayList<Integer>();
			
			int cnt1=0,cnt2=0;
			System.out.println("JobHandler :: MakeFinalToJSON Getinputfile >> Success : " +cnt1 + " " + cnt2);
			cnt1 = 0; cnt2 = 0;
			String s = "";
			while (inputStream.available()>0)
			{
			    s = inputStream.readLine();	
			    StringTokenizer token =  new StringTokenizer(s," ,\n");
			    cnt2 = 0;
				
				while(token.hasMoreTokens())
				{
					data[cnt1][cnt2] = Double.parseDouble(token.nextToken());
					cnt2++;
				}
				ClusterData cdata = new ClusterData(data[cnt1]);
				KmeansCluster kmeans = new KmeansCluster(numElements);
				
				int centroidIndex = kmeans.getNearestCentroid(cdata, centroids);
				link[centroidIndex].add(cnt1);	
				counter[centroidIndex]++;
			    cnt1++;			    
			} 
			inputStream.close();
			
	 		
	 		double[] dataElements = new double[numElements];
	 		KmeansCluster kmeans = new KmeansCluster(numElements);
	 		//ClusterData cdata = new ClusterData();
			//ClusterCentroids tempCentroids = centroidsHash.get(fromK+i); 		
				
			JSONObject jsonObj = new JSONObject();
			jsonObj.put("k", K );
			jsonObj.put("dimension", Config.getInstance().Dimension);
			
			JSONArray jsoncollection = new JSONArray();
			int i,j;
			for(i=0;i<K;i++)
			{
				double [] dtemp = null;
				JSONObject jrow = new JSONObject();
				JSONArray jArr = new JSONArray();
				for(j=0;j<numElements;j++)
				{
					dtemp = 	centroids.getCentroid(i);
					if(dtemp == null)
					{
						System.out.println("not enough centroids ");
						break;
					}
					System.out.println("centroids number " + j + "   data : "+dtemp[0] + " " + dtemp[1]);
					jArr.put(centroids.getCentroid(i)[j]);
				}
				if(dtemp == null)
				{
					break;
				}
				jrow.put("center",jArr);
				jrow.put("count",counter[i]);
				
				JSONObject jpoints = new JSONObject();
				JSONArray jpointsarray = new JSONArray();
				for(j=0;j<counter[i];j++)
				{
					int index = link[i].get(j);
					dataElements = data[index];
				//	System.out.println("K = " + K +" cluster = " + i + " dataElements = " + dataElements[0] + " , " + dataElements[1]);
					JSONArray jparray = new JSONArray();
					jpointsarray.put(dataElements);				
				}
				
				jrow.put("points", jpointsarray);
				jsoncollection.put(jrow);
			}
			jsonObj.put("clusters" , jsoncollection);
			System.out.println(jsonObj.toString());

			String outputpath = Config.getInstance().OutputDir + "Final.json";
			hdfsmanager = new HDFSManager(conf ,FileSystem.get(URI.create(outputpath),conf) );
			if(!hdfsmanager.exists(outputpath))
			{
				hdfsmanager.delete(new Path(outputpath+"_backup"),true);
				hdfsmanager.rename(new Path(outputpath), new Path(outputpath + "_backup"));
				hdfsmanager.delete(new Path(outputpath),true);
			}
			FSDataOutputStream out = hdfsmanager.getFileSystem().create(new Path(outputpath));
			out.write(jsonObj.toString().getBytes("UTF-8"));
			out.close();
			
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("------- bad k value!---------");
		}
	}

}
