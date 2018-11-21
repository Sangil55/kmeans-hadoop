package com.swinno.hadoop.cluster;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.swinno.hadoop.common.Config;
import com.swinno.util.math.Silhouette;

public class SilhouetteChecker {
	private String inputpath;
	private String clusterpointpath;
	private int K;
	SilhouetteChecker(String inputpath,String clusterpointpath,int K)
	{
		this.inputpath = inputpath;
		this.clusterpointpath =clusterpointpath;
		this.K = K;
	}
	
	public double checkbyMean()
	{
		System.out.println("SilhouetteChecker do the job by Mean > K: " + K + " , Path : "  + clusterpointpath);
		String inputpath = Config.getInstance().InputDir; 
		Configuration conf = new Configuration();
		int numElements = Config.getInstance().Dimension;
		int fromK = Config.getInstance().K_RangeStart;
		int toK = Config.getInstance().K_RangeEnd;
		int col = Config.getInstance().col;
		String centroiddir = Config.getInstance().CentroidDir;
		FileSystem fs;
		double silerror = 0,sumerror = 0;
		
		try {
			fs = FileSystem.get(URI.create(inputpath),conf);
			//hdfsmanager = new HDFSManager(conf ,FileSystem.get(URI.create(inputpath),conf) );
			
			/////////////1. get centriods
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
	 		
	 		/////////////////////2. get input data
	 		if(!fs.exists(new Path(inputpath)))
			{	
				System.out.println("----- There is no directory on "+inputpath);
				return 0.0;
			}
			
			FileStatus []status = fs.listStatus(new Path(inputpath)); 
			for(int i =0;i<status.length;i++)
			{
				System.out.println(status[i].getPath().toString());
			}
		
			FSDataInputStream inputStream;
			inputStream = fs.open(status[0].getPath());

			String outputdir = Config.getInstance().OutputDir;
			String outputpath = "";
			if(outputdir.charAt(outputdir.length()-1) == '/')
				outputpath= outputdir + "silhouette" + K + ".csv";
			else
				outputpath= outputdir + "/silhouette" + K + ".csv";
			if(fs.exists(new Path(outputpath)) )
			{
				fs.delete(new Path(outputpath),true);
			}
			//fs.mkdirs(new Path(outputdir));
			FSDataOutputStream out = fs.create(new Path(outputpath));
			
			
			int cnt1=0,cnt2=0;
			
			String s = "";
			while (inputStream.available()>0)
			{
			    s = inputStream.readLine();	
			    StringTokenizer token =  new StringTokenizer(s," ,\n");
			    cnt2 = 0;
				double data[] = new double[col];
				while(token.hasMoreTokens())
				{
					data[cnt2++] = Double.parseDouble(token.nextToken());
				}
				ClusterData cdata = new ClusterData(data);
				KmeansCluster kmeans = new KmeansCluster(numElements);
				
				int centroidIndex = kmeans.getNearestCentroid(cdata, centroids);
				int centroidIndex_second = kmeans.getSecondNearestCentroid(cdata, centroids);
								
				Silhouette sil = new Silhouette();
				silerror = sil.getsilebyMean(data,centroids.getCentroid(centroidIndex),centroids.getCentroid(centroidIndex_second));
				
				String outer =  centroidIndex + "," + centroidIndex_second + "," + String.valueOf(silerror);
				out.write(outer.getBytes("UTF-8"));
				out.write("\n".getBytes("UTF-8"));
				
				sumerror += silerror;
				
			    cnt1++;			    
			}
			out.close();
			inputStream.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("------- error at silouette checker---------");
			return 0.0;
		}
		return sumerror;
	}
	public double checkbysampling()
	{
				
		return 0.0;
	}
	public double checkbyall()
	{
				
		return 0.0;
	}
}
