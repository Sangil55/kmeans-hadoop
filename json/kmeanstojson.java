package com.swinno.util.json;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONArray;
import org.json.JSONObject;

import com.swinno.hadoop.cluster.ClusterCentroids;
import com.swinno.hadoop.cluster.ClusterData;
import com.swinno.hadoop.cluster.KmeansCluster;
import com.swinno.hadoop.common.Config;

/**
 * Hello world!
 *
 */
public class kmeanstojson 
{
	
	public static boolean setConfFromConfFile() throws IOException
	{
		
		Configuration conf = new Configuration();
		conf.setBoolean("fs.hdfs.impl.disable.cache",true);
		Path confpath = new Path(Config.getInstance().ConfPath);
		FileSystem fs = FileSystem.get(URI.create(confpath.toString()),conf);
		try {
			//hdfsmanager = new HDFSManager(conf ,FileSystem.get(URI.create(confpath),conf) );
		
			if(!fs.exists(confpath))
				return false;
			
			FSDataInputStream inputStream;
			
			inputStream = fs.open(confpath);
			
			while (inputStream.available()>0)
			{
			    String s,key,value,value2;
			    s = inputStream.readLine()	;
			    StringTokenizer strtoken =  new StringTokenizer(s," ,");
			    if(!strtoken.hasMoreTokens())
			    	break;
			    key = strtoken.nextToken();
			    value = strtoken.nextToken();
			    if( (key.compareTo("K_Range") == 0) && strtoken.hasMoreTokens())
			    {
			    	value2 = strtoken.nextToken();
			    	Config.getInstance().setValue(key,value,value2);
			    }
			    else
			    	Config.getInstance().setValue(key,value);
		   } 
			inputStream.close();
//			hdfsmanager.getFileSystem().close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return false;
		}
		return true;
	}
	
	public static void makeFinal( String strmode )
	{
		//StringTokenizer tokens = 
		//if( strmode == 
		int i,j;
		for(i=Config.getInstance().K_RangeStart;i<=Config.getInstance().K_RangeEnd;i++)
		{
			MakeFinalToJSON(i);
		}
	}
	
	
	private static void MakeFinalToJSON(int K)
	{
		System.out.println("Make Json Start with K : " + K);
		String inputpath = Config.getInstance().InputDir; 
		Configuration conf = new Configuration();
		int numElements = Config.getInstance().Dimension;
		int fromK = Config.getInstance().K_RangeStart;
		int toK = Config.getInstance().K_RangeEnd;
		String centroiddir = Config.getInstance().CentroidDir;
		FileSystem fs;
		try {
			fs = FileSystem.get(URI.create(inputpath),conf);
			//hdfsmanager = new HDFSManager(conf ,FileSystem.get(URI.create(inputpath),conf) );
			/////////////first get centriods
			int numPos = K%(toK-fromK+1)/10;
	        
	        String fileIndex = "";
	        for(int i=0;i<4-numPos;i++)
	        {
	        	fileIndex += "0";
	        }
	        fileIndex = fileIndex+(K%(toK-fromK+1));
	
			//FileSystem fs = hdfsmanager.getFileSystem();
	        // centriod read should be done at data/kmeans/cluster10.data
			System.out.println( "Final Centroid path : "+centroiddir+"clusters"+K+".data" );
		    DataInputStream d = new DataInputStream(fs.open(new Path(centroiddir+"clusters"+K+".data")));
	        BufferedReader reader = new BufferedReader(new InputStreamReader(d));
	        String line = null;
	        ClusterCentroids centroids;
	        centroids = new ClusterCentroids(K);
			
			while ((line = reader.readLine()) != null){
		        String token[] = line.split(",");
				double[] elements = new double[numElements];
			 	for(int i=0;i<numElements;i++)
				{
					elements[i] = Double.parseDouble(token[i+1]);
				}
				centroids.addCentroid(elements);
			}
	 		reader.close();
	 		
	 		//System.out.println(centroids.getK());
	 		////////////second get data and link with cluster 
			if(!fs.exists(new Path(inputpath)))
			{	
				System.out.println("----- There is no directory on "+inputpath);
				return;
			}
			
			FileStatus []status = fs.listStatus(new Path(inputpath)); 
			for(int i =0;i<status.length;i++)
			{
				System.out.println(status[i].getPath().toString());
			}
		
			FSDataInputStream inputStream;
			inputStream = fs.open(status[0].getPath());
			
			int cnt1=0,cnt2=0;
			String s = "";
			while (inputStream.available()>0)
			{
			    s = inputStream.readLine();
			    cnt1++;
			} 
			StringTokenizer strtoken =  new StringTokenizer(s," ,");
			while(strtoken.hasMoreTokens())
			{
				cnt2++;
				strtoken.nextToken();
			}
			inputStream.close();
			inputStream = fs.open(status[0].getPath());
			//inputStream.reset();
			Config.getInstance().setValue("row", cnt1);
			Config.getInstance().setValue("col", cnt2);
			double data[][] = new double[Config.getInstance().row][Config.getInstance().col];
			
			//ClusterData Finaldata[] = new ClusterData[Config.getInstance().row];
			int counter[] = new int[K];
			List <Integer> []link = new List[K];
			for(int i=0;i<K;i++) 	
				link[i] = new ArrayList<Integer>();
			
			System.out.println("JobHandler :: MakeFinalToJSON Getinputfile >> Success : " +cnt1 + " " + cnt2);
			cnt1 = 0; cnt2 = 0;
			while (inputStream.available()>0)
			{
			    s = inputStream.readLine();	
			  //  inputStream.
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
			//System.out.println(jsonObj.toString());

			String outputpath = Config.getInstance().OutputDir + "final/" + "final_k" + K + ".json";
			fs = FileSystem.get(URI.create(outputpath),conf);
			if(!fs.exists(new Path(outputpath)))
			{
				fs.delete(new Path(outputpath+"_backup"),true);
				fs.rename(new Path(outputpath), new Path(outputpath + "_backup"));
				fs.delete(new Path(outputpath),true);
			}
			FSDataOutputStream out = fs.create(new Path(outputpath));
			out.write(jsonObj.toString().getBytes("UTF-8"));
			out.close();
			data = null;
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("------- bad k value!---------");
		}
	}
    /*public static void main( String[] args )
    {
    	try {
			setConfFromConfFile();
			makeFinal(args[0]);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    }*/
}
