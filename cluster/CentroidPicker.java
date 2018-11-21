package com.swinno.hadoop.cluster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.mapred.JobConf;

import com.swinno.util.math.Distance;
import com.swinno.util.math.EuclideanDistance;

/**
 * 
 * @author Yonghee,Lee
 * @brief init centroids by kmeans++ algorithm
 */
public class CentroidPicker {
	
	private String inputDir;
	private int fromK;
	private int toK;
	private int numElements;
	
	public CentroidPicker(String inputDir, int fromK, int toK, int numElements)
	{
		this.inputDir = inputDir;
		this.fromK = fromK;
		this.toK = toK;
		this.numElements = numElements;
	}
	
	public void startGenerate() throws InterruptedException
	{
		
		ArrayList<Thread> threadList = new ArrayList<Thread>();
		
		for(int i=fromK;i<(toK+1);i++)
		{
			Runnable r = new GenerateCentroidsRunnable(inputDir, fromK,toK,numElements,i);
			Thread t = new Thread(r);
			t.start();
			threadList.add(t);
		}
		
		for(Thread t : threadList)
		{
			t.join();
		}
		System.out.println("Making initial Centroids is done-------------");
	}
	
	public void generateCentroids(int fromK, int toK, int numElements) throws IOException
	{
		Random r = new Random();
		JobConf conf = new JobConf();
		conf.setBoolean("fs.hdfs.impl.disable.cache",true);
		FileSystem fs = FileSystem.get(conf);

	    int kVal = fromK;
	    
	    while(kVal<=toK)
	    {
	    	int tempElement[] = new int[numElements];
	    	StringBuffer sb = new StringBuffer();
	    	
	    	
//	    	BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(workDir.toString() +"/data/kmeans/clusters"+kVal+".data"), true)));
	    	BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path("data/kmeans/clusters"+kVal+".data"), true)));

	    	for(int i=0;i<kVal;i++)
	    	{
	    		sb.append("C");
	    		sb.append(i+1);
	    		sb.append("-"+kVal+",");
	    		
	    		for(int j=0;j<numElements;j++)
	    		{
	    			tempElement[j]=(int)(Math.random()*(999999-100000+1)+100000);
	    			sb.append(tempElement[j]);
	    			if(j != (numElements-1))
	    				sb.append(",");
	    		}
		    	bw.write(sb.toString());
	            bw.write("\n");
	            sb = new StringBuffer();

	    	}
	    	bw.close();
	    	kVal++;
            
	    }
	    
	    
	}

	public void getCentroidsByKmeansPP(String InputDir, int k, int numElements) throws IOException
    {
    	/*
    	 *  1. get random point from N points
    	 *  2. get Max distance point with 1. point from N-1 points
    	 *  3. get Max distance point with 2. point from N-2 points
    	 *  4. repeat step 2,3 until getting K centroids
    	 */ 
		
		Map<String,double[]> randomPoint = new HashMap<String,double[]>();
		JobConf conf = new JobConf();
		conf.setBoolean("fs.hdfs.impl.disable.cache",true);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fStatus = fs.listStatus(new Path(InputDir));
		String[] inputFileList = new String[fStatus.length]; 
		
		//get file lists under input directory
		for(int i=0; i<fStatus.length;i++)
		{
			System.out.println("Files Under Input Directory: "+fStatus[i].getPath().toString());

			String[] tokens = fStatus[i].getPath().toString().split(":");
			if(tokens[0] != null && tokens[0].equals("file"))
				inputFileList[i] = tokens[1];
			else if(tokens[0] != null && tokens[0].equals("hdfs"))
				inputFileList[i] = fStatus[i].getPath().toString();
		}
		
		//iterate to get data from input files until get Kth initial centroid
		String randomPivotPointKey = "";
		String randomPointKey = "";
		double[] mostFarPoint = null;
		Random r = new Random(System.currentTimeMillis());
		int randomCount = r.nextInt(k);
		double maxDistance = -1.0;
		
		while(randomPoint.size() != k)
		{	
			int readCount = 0;
			boolean pickRandomDone = false;
			for(int j=0; j<inputFileList.length;j++)
			{
				DataInputStream d = new DataInputStream(fs.open(new Path(inputFileList[j])));
				BufferedReader reader = new BufferedReader(new InputStreamReader(d));
				String line = null;
				
				int lineCount = 0;
				
				while ((line = reader.readLine()) != null)
				{
					StringTokenizer itr = new StringTokenizer(line);
					int index = 0;
					double[] dataElements = new double[numElements];
//					System.out.println("read line: "+line);
					while(index < numElements && itr.hasMoreTokens()) 
					{
						dataElements[index] = Double.parseDouble(itr.nextToken());
						index++;
					}
				
					//get first random point
					if(lineCount == randomCount && !pickRandomDone)
					{
						randomPivotPointKey = inputFileList[j]+";"+lineCount;
						System.out.println("-----------"+k+"'s first random point position: " + randomPivotPointKey);
						randomPoint.put(randomPivotPointKey,dataElements);
						randomCount = -1;
						lineCount = 0;
						reader = new BufferedReader(new InputStreamReader(d));
						pickRandomDone = true;
						continue;
					}
				
					//if random point count is more than 1 and less than k, calculate max distance for getting next random point
					if(0 < randomPoint.size() && randomPoint.size() < k)
					{
						Distance distance = new EuclideanDistance();
						double tempDistance = distance.getDistance(dataElements, randomPoint.get(randomPivotPointKey));
				
						if(tempDistance > maxDistance)
						{
							if(!randomPoint.containsKey(inputFileList[j]+";"+lineCount))
							{	
								List<String> pointList = new ArrayList<String>(randomPoint.keySet());
								boolean skip = false;
								int sameCount = 0;
								for(String point : pointList)
								{
									double[] tempPoint = randomPoint.get(point);
									
									for(int l=0;l<tempPoint.length;l++)
									{
										if(tempPoint[l] == dataElements[l])
											sameCount++;
									}
									if(sameCount == numElements)
										skip = true;									
								}
								
								if(!skip)
								{
									maxDistance = tempDistance;
									randomPointKey = inputFileList[j] + ";" + lineCount;
									mostFarPoint = new double[numElements];
									System.arraycopy(dataElements,0,mostFarPoint,0,numElements);
								}
							}
						}
					}
					lineCount++;
					
				}
				readCount++;

				System.out.println("------------ file read Count: " + readCount);
				reader.close();
			}
			String checkStr = "";
			for(int i=0;i<numElements;i++)
			{
				checkStr = checkStr + " " + mostFarPoint[i];
			}
			System.out.println("-----------"+k+" random point position: " + randomPointKey + checkStr);
			randomPoint.put(randomPointKey, mostFarPoint);
			randomPivotPointKey = randomPointKey;
			maxDistance = -1.0;
		}
		
		writeCentroid(k,numElements,randomPoint); 
    }
	
	public void writeCentroid(int k, int numElements, Map<String,double[]> randomPoint) throws IllegalArgumentException, IOException
	{
		JobConf conf = new JobConf();
		conf.setBoolean("fs.hdfs.impl.disable.cache",true);
		FileSystem fs = FileSystem.get(conf);

	    StringBuffer sb = new StringBuffer();	    	
	    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path("data/kmeans/clusters"+k+".data"), true)));
	    
	    List<String> keys = new ArrayList<String>(randomPoint.keySet());
	    
	    for(int i=0;i<randomPoint.size();i++)
	    {	
	    	sb = new StringBuffer();
	    	sb.append("C");
    		sb.append(i+1);
    		sb.append("-"+(i+1)+",");
	    	
	    	for(int j=0;j<numElements;j++)
	    	{
	    		sb.append(randomPoint.get(keys.get(i))[j]);
	    		if(j != (numElements-1))
    				sb.append(",");
	    	}
	    	bw.write(sb.toString());
	    	bw.write("\n");
	    }
	    bw.close();
	 
	}
	public void writeCentroidPlus(int k, int numElements, Map<Integer,double[]> randomPoint) throws IllegalArgumentException, IOException
	{
		JobConf conf = new JobConf();
		conf.setBoolean("fs.hdfs.impl.disable.cache",true);
		FileSystem fs = FileSystem.get(conf);

	    StringBuffer sb = new StringBuffer();	    	
	    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path("data/kmeans/clusters"+k+".data"), true)));
	    
	    
	    for(int i=0;i<randomPoint.size();i++)
	    {	
	    	sb = new StringBuffer();
	    	sb.append("C");
    		sb.append(i+1);
    		sb.append("-"+(i+1)+",");
	    	
	    	for(int j=0;j<numElements;j++)
	    	{
	    		sb.append(randomPoint.get(i)[j]);
	    		if(j != (numElements-1))
    				sb.append(",");
	    	}
	    	bw.write(sb.toString());
	    	bw.write("\n");
	    }
	    bw.close();
	 
	}
 
	/**
	 * @brief initialize centroids using kmeans++ and advanced method
	 * @param InputDir
	 * @param k
	 * @param numElements
	 * @throws IOException
	 */
	public void initCentroidsKmeanspp(String InputDir, int k, int numElements) throws IOException
	{
		
		JobConf conf = new JobConf();
		conf.setBoolean("fs.hdfs.impl.disable.cache",true);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fStatus = fs.listStatus(new Path(InputDir));
		String[] inputFileList = new String[fStatus.length]; 
		
		//get file lists under input directory
		for(int i=0; i<fStatus.length;i++)
		{
			System.out.println("Files Under Input Directory: "+fStatus[i].getPath().toString());

			String[] tokens = fStatus[i].getPath().toString().split(":");
			System.out.println(fStatus[i].getPath().toString());
			if(tokens[0] != null && tokens[0].equals("file"))
				inputFileList[i] = tokens[1];
			else if(tokens[0] != null && tokens[0].equals("hdfs"))
				inputFileList[i] = fStatus[i].getPath().toString();
		}
		
		//iterate to get data from input files until get Kth initial centroid
		double[] mostFarPoint = null;
		Random r = new Random(k*k);
		int randomCount = -1;
//		randomCount = r.nextInt(k*100);
		double distribute = 0.0;
		double maxDistribute = 0.0;
		
		Map<Integer,double[]> initCentroids = new HashMap<Integer,double[]>();
		int sumlines = 0;
		double[] sumElements = new double[numElements];		
		for(int j=0; j<inputFileList.length;j++)
		{
			DataInputStream d = new DataInputStream(fs.open(new Path(inputFileList[j])));
			BufferedReader reader = new BufferedReader(new InputStreamReader(d));
			String line = null;
			
			
			while ((line = reader.readLine()) != null)
			{
				StringTokenizer itr = new StringTokenizer(line);
				int index = 0;
				double[] dataElements = new double[numElements];
//				System.out.println("read line: "+line);
				while(index < numElements && itr.hasMoreTokens()) 
				{
					dataElements[index] = Double.parseDouble(itr.nextToken());
					index++;
				}
				
				for(int i=0;i<numElements;i++)
				{
					sumElements[i] = sumElements[i] + dataElements[i];
				}
				sumlines++;
			}
		}
		double grandMean[] = new double[numElements];
		
		for(int m=0;m<numElements;m++)
		{
			grandMean[m] = sumElements[m]/sumlines;
		}
//		
		initCentroids.put(0,grandMean);

		boolean first = true;
		while(initCentroids.size() < k)
		{	
				
			for(int j=0; j<inputFileList.length;j++)
			{
				DataInputStream d = new DataInputStream(fs.open(new Path(inputFileList[j])));
				BufferedReader reader = new BufferedReader(new InputStreamReader(d));
				String line = null;
					
				while ((line = reader.readLine()) != null)
				{
					StringTokenizer itr = new StringTokenizer(line);
					int index = 0;
					double[] dataElements = new double[numElements];
//					System.out.println("read line: "+line);
					while(index < numElements && itr.hasMoreTokens()) 
					{
						dataElements[index] = Double.parseDouble(itr.nextToken());
						index++;
					}
						
					Distance distance = new EuclideanDistance();
					for(int i=0;i<initCentroids.size();i++)
					{
						double dupCheck = 0;
//						System.out.println("K"+k+" picked random points ----: "+ randomPoint.get(i)[0] + ","+randomPoint.get(i)[1]);
						for(int l=0;l<numElements;l++)
						{								
							dupCheck = dupCheck + Math.sqrt(Math.pow((initCentroids.get(i)[l] - dataElements[l]),2));
						}
						if(dupCheck > 0)
						{
							double tempDistance = distance.getDistanceSquare(initCentroids.get(i), dataElements);
							distribute = distribute + tempDistance;
						}
						else
						{
							distribute = 0.0;
							break;
						}
					}
						
					if(maxDistribute < distribute)
					{
						maxDistribute = distribute;
						mostFarPoint = new double[numElements];
						System.arraycopy(dataElements,0,mostFarPoint,0,numElements);
					}
											
				}
				
			}
			if(initCentroids.size()>=1)
			{
				if(first)
				{
					initCentroids.clear();
					first = false;
				}
				initCentroids.put(initCentroids.size(), mostFarPoint);
				maxDistribute = 0;
				mostFarPoint = new double[numElements];
			}
		}
		writeCentroidPlus(k,numElements,initCentroids);
	}
	
	public void initCentroidsKmeansppDistDelta(String InputDir, int k, int numElements) throws IOException
	{
		
		JobConf conf = new JobConf();
		conf.setBoolean("fs.hdfs.impl.disable.cache",true);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fStatus = fs.listStatus(new Path(InputDir));
		String[] inputFileList = new String[fStatus.length]; 
		
		//get file lists under input directory
		for(int i=0; i<fStatus.length;i++)
		{
			System.out.println("Files Under Input Directory: "+fStatus[i].getPath().toString());

			String[] tokens = fStatus[i].getPath().toString().split(":");
			if(tokens[0] != null && tokens[0].equals("file"))
				inputFileList[i] = tokens[1];
		}
		
		//iterate to get data from input files until get Kth initial centroid
		double[] mostFarPoint = null;
		Random r = new Random(k*k);
		int randomCount = -1;
//		randomCount = r.nextInt(k*100);
		double distribute = 0.0;
		double maxDistribute = 0.0;
		double distDelta = 0.0;
		double numK = k;
//		double divisionFactor = (Math.sqrt(numElements)-1)*numK;
		
		Map<Integer,double[]> initCentroids = new HashMap<Integer,double[]>();
		int sumlines = 0;
		double[] sumElements = new double[numElements];		
		for(int j=0; j<inputFileList.length;j++)
		{
			DataInputStream d = new DataInputStream(fs.open(new Path(inputFileList[j])));
			BufferedReader reader = new BufferedReader(new InputStreamReader(d));
			String line = null;
			
			
			while ((line = reader.readLine()) != null)
			{
				StringTokenizer itr = new StringTokenizer(line);
				int index = 0;
				double[] dataElements = new double[numElements];
//				System.out.println("read line: "+line);
				while(index < numElements && itr.hasMoreTokens()) 
				{
					dataElements[index] = Double.parseDouble(itr.nextToken());
					index++;
				}
				
				for(int i=0;i<numElements;i++)
				{
					sumElements[i] = sumElements[i] + dataElements[i];
				}
				sumlines++;
			}
		}
		double grandMean[] = new double[numElements];
		
		for(int m=0;m<numElements;m++)
		{
			grandMean[m] = sumElements[m]/sumlines;
		}
//		
		initCentroids.put(0,grandMean);

		boolean first = true;
		while(initCentroids.size() < k)
		{	
				
			for(int j=0; j<inputFileList.length;j++)
			{
				DataInputStream d = new DataInputStream(fs.open(new Path(inputFileList[j])));
				BufferedReader reader = new BufferedReader(new InputStreamReader(d));
				String line = null;
					
				while ((line = reader.readLine()) != null)
				{
					StringTokenizer itr = new StringTokenizer(line);
					int index = 0;
					double[] dataElements = new double[numElements];
//					System.out.println("read line: "+line);
					while(index < numElements && itr.hasMoreTokens()) 
					{
						dataElements[index] = Double.parseDouble(itr.nextToken());
						index++;
					}
						
					Distance distance = new EuclideanDistance();
					for(int i=0;i<initCentroids.size();i++)
					{
						double dupCheck = 0;
//						System.out.println("K"+k+" picked random points ----: "+ randomPoint.get(i)[0] + ","+randomPoint.get(i)[1]);
						for(int l=0;l<numElements;l++)
						{								
							dupCheck = dupCheck + Math.sqrt(Math.pow((initCentroids.get(i)[l] - dataElements[l]),2));
						}
						if(dupCheck > 0)
						{
							double tempDistance = distance.getDistance(initCentroids.get(i), dataElements);
							if(!first)
								if(tempDistance < distDelta)
								{
									distribute = 0.0;
									break;
								}
							distribute = distribute + Math.pow(tempDistance,2);
						}
						else
						{
							distribute = 0.0;
							break;
						}
					}
						
					if(maxDistribute < distribute)
					{
						maxDistribute = distribute;
						mostFarPoint = new double[numElements];
						System.arraycopy(dataElements,0,mostFarPoint,0,numElements);
					}
											
				}
				
			}
			if(initCentroids.size()>=1)
			{
				if(first)
				{
					initCentroids.clear();
					first = false;
					Distance distance = new EuclideanDistance();
//					distDelta = Math.sqrt((Math.pow(distance.getDistance(grandMean, mostFarPoint),2)*Math.PI*4)/(6*Math.tan(Math.PI/6)*numK));
					double distMean = distance.getDistance(grandMean, mostFarPoint);
					distDelta = ((6*numK/Math.PI*Math.tan(Math.PI/6)-2*Math.pow(distMean,2)) + 
									Math.sqrt(Math.pow((6*numK/Math.PI*Math.tan(Math.PI/6)-2*Math.pow(distMean,2)),2) - 4*Math.pow(Math.cos(Math.PI/6), 2)*Math.pow(distMean, 2)))/(2*Math.pow(distMean, 2));
				}
				
//				distDelta = (distance.getDistance(grandMean, mostFarPoint)*Math.PI)/divisionFactor;
		
				initCentroids.put(initCentroids.size(), mostFarPoint);
				maxDistribute = 0;
				mostFarPoint = new double[numElements];
			}
			
			//centroids duplication check
			//if duplication exists, clear initCentroids and add 1 to division facter
			if(initCentroids.size() == k)
			{
				String centStr = "";
				TreeSet<String> dupChecker = new TreeSet<String>();
				for(int i=0;i<initCentroids.size();i++)
				{
					for(int c=0;c<numElements;c++)
					{
						centStr = centStr + initCentroids.get(i)[c];
					}
					System.out.println(k+" cent value---------------: "+centStr);
					dupChecker.add(centStr);
					centStr = "";
				}
				
				if(dupChecker.size() < initCentroids.size())					
				{
					System.out.println("Wrong Centroid exist-------------: "+k);
					initCentroids.clear();
					numK++;
					initCentroids.put(0,grandMean);
					first = true;
				}
			}
		}
		
		
		
		writeCentroidPlus(k,numElements,initCentroids);
	}
	
	class GenerateCentroidsRunnable implements Runnable
	{

		private String inputDir;
		private int fromK;
		private int toK;
		private int numElements;
		private int k;
		
		public GenerateCentroidsRunnable(String inputDir, int fromK, int toK, int numElements, int k)
		{
			this.inputDir = inputDir;
			this.fromK = fromK;
			this.toK = toK;
			this.numElements = numElements;
			this.k = k;
		}
		
		@Override
		public void run() {
			
			CentroidPicker generator = new CentroidPicker(inputDir, fromK, toK, numElements);
			
			try {
//				generator.getCentroidsByKmeansPP(inputDir, k, numElements);
//				generator.initCentroidsKmeanspp(inputDir, k, numElements);
				generator.initCentroidsKmeansppDistDelta(inputDir, k, numElements);
			} catch (IOException e) {
				
				e.printStackTrace();
			}
			
		}
		
	}
}
