package com.swinno.hadoop.cluster;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import com.swinno.util.math.Distance;
import com.swinno.util.math.EuclideanDistance;

public class FileManager {
	
	public BufferedReader getResultData(int k, int fromK, int toK, String outputDir) throws IOException
	{
		JobConf conf = new JobConf();		
		conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		FileSystem fs = FileSystem.get(conf);
		
        int numPos = k%(toK-fromK+1)/10;
        
        String fileIndex = "";
        for(int i=0;i<4-numPos;i++)
        {
        	fileIndex += "0";
        }
        fileIndex = fileIndex+(k%(toK-fromK+1));
        
        
        DataInputStream d = new DataInputStream(fs.open(new Path(outputDir + "/part-r-"+fileIndex)));
        BufferedReader reader = new BufferedReader(new InputStreamReader(d));
       
		return reader;
		
	}
	
	public List<double[]> getInputData(String InputDir, int numElements) throws IllegalArgumentException, IOException
	{
		JobConf conf = new JobConf();
		conf.setBoolean("fs.hdfs.impl.disable.cache",true);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fStatus = fs.listStatus(new Path(InputDir));
		String[] inputFileList = new String[fStatus.length]; 
		List<double[]> pointData = new ArrayList<double[]>();
		
		//get file lists under input directory
		for(int i=0; i<fStatus.length;i++)
		{

			String[] tokens = fStatus[i].getPath().toString().split(":");
			if(tokens[0] != null && tokens[0].equals("file"))
				inputFileList[i] = tokens[1];
			else if(tokens[0] != null && tokens[0].equals("hdfs"))
				inputFileList[i] = fStatus[i].getPath().toString();
		}
		
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
//			    System.out.println("SSE read line: "+line);
				while(index < numElements && itr.hasMoreTokens()) 
				{
					String temp = itr.nextToken();
//					System.out.println("get input data parsing working? ------1: "+temp);
					dataElements[index] = Double.parseDouble(temp);
//					System.out.println("get input data parsing working? ------2: "+dataElements[index]);
					index++;
				}
				pointData.add(dataElements);
			}
			reader.close();
		}
		
		return pointData;
	}

}
