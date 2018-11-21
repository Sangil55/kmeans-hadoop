package com.swinno.hadoop.kmeans;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.swinno.hadoop.common.Common;
import com.swinno.hadoop.common.Config;
import com.swinno.hadoop.kmeans.HDFSManager;

public class PresetKmean {
	private String confpath;
	private String inputFilePath;
	private long row;
	private int col;
	private int [][] data;
	public PresetKmean() {
		// confpath (default) $(hadoop homedirectory)/kmean/conf
		confpath = Config.getInstance().ConfPath; 
	}
	
	public void init() throws InterruptedException
	{
		try {
			setConfFromConfFile();
			getSourceData();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}       
		  int k_itr;
       for(k_itr=Config.getInstance().K_RangeStart;k_itr<=Config.getInstance().K_RangeEnd;k_itr++)
       {
	 //      setMiddlePointbySeperation(k_itr);
	    //   setMiddlePointbyRandom(k_itr);
       }
	}
	
	public void setInputFilePath(String path)
	{
		inputFilePath = path;
	}
		
	@SuppressWarnings({ "deprecation", "resource" })
	public boolean setConfFromConfFile() throws IOException
	{
		
		Configuration conf = new Configuration();
		conf.setBoolean("fs.hdfs.impl.disable.cache",true);
		HDFSManager hdfsmanager;
		try {
			hdfsmanager = new HDFSManager(conf ,FileSystem.get(URI.create(confpath),conf) );
		
			if(!hdfsmanager.exists(confpath))
				return false;
			
			FSDataInputStream inputStream;
			
			inputStream = hdfsmanager.getFileSystem().open(new Path(confpath));
			
			while (inputStream.available()>0)
			{
			    String s,key,value,value2;
			    s = inputStream.readLine();
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
	private void sourceDataToInputdata(int d[][])
	{
		String outputdir = Config.getInstance().InputDir;
		String outputpath = "";
		if(outputdir.charAt(outputdir.length()-1) == '/')
			outputpath= Config.getInstance().InputDir + "inputdata";
		else
			outputpath= Config.getInstance().InputDir + "/inputdata";
		Config.getInstance().InputFirst_FilePath = outputpath;
		long n = Config.getInstance().row;
		int m = Config.getInstance().col;
		n = this.row;
		m = this.col;
		Configuration conf = new Configuration();
		HDFSManager hdfsmanager;
		try {
			hdfsmanager = new HDFSManager(conf ,FileSystem.get(URI.create(outputpath),conf) );
			if(!hdfsmanager.exists(outputpath))
			{
				hdfsmanager.delete(new Path(outputpath));
			}
			hdfsmanager.getFileSystem().mkdirs(new Path(outputdir));
			FSDataOutputStream out = hdfsmanager.getFileSystem().create(new Path(outputpath));
			for(int i=0;i<n;i++)
			{
				for(int j =0;j<m;j++)
				{
					out.write(String.valueOf(d[i][j]).getBytes("UTF-8"));
					out.write(" ".getBytes("UTF-8"));					
					//out.write(" ");
				}
				out.write("\n".getBytes("UTF-8"));				
			}
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally
		{
			
		}
		
	}
	private void sourceDataToInputdata2(double [] data, FSDataOutputStream outpStream, boolean bwriteEnter) throws UnsupportedEncodingException, IOException
	{
		int m = this.col;
		for(int j =0;j<m;j++)
		{
			outpStream.write(String.valueOf(data[j]).getBytes("UTF-8"));
			outpStream.write(" ".getBytes("UTF-8"));			
		}
		if(bwriteEnter)
			outpStream.write("\n".getBytes("UTF-8"));	
	}
	@SuppressWarnings({ "deprecation", "resource" })
	public void getSourceData() throws IOException
	{
		String inputpath = Config.getInstance().SourcePath; 
		Configuration conf = new Configuration();
		HDFSManager hdfsmanager;
		int cnt1=0,cnt2=0;
		try {
			hdfsmanager = new HDFSManager(conf ,FileSystem.get(URI.create(inputpath),conf) );
		
			if(!hdfsmanager.exists(inputpath))
			{	
				System.out.println("----- There is no file on "+inputpath);
				return;
			}
			FSDataInputStream inputStream;
			
			inputStream = hdfsmanager.getFileSystem().open(new Path(inputpath));
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
			inputStream.seek(0);
			Config.getInstance().setValue("row", cnt1);
			Config.getInstance().setValue("col", cnt2);
			Config.getInstance().col = cnt2;
			
			this.row = cnt1;
			this.col = cnt2;
			
			System.out.println("PresetKmean :: Getinputfile >> Finally data count : " +cnt1 + " " + cnt2);
			cnt1 = 0; cnt2 = 0;
			
			String outputdir = Config.getInstance().InputDir;
			String outputpath = "";
			if(outputdir.charAt(outputdir.length()-1) == '/')
				outputpath= Config.getInstance().InputDir + "inputdata";
			else
				outputpath= Config.getInstance().InputDir + "/inputdata";
			Config.getInstance().InputFirst_FilePath = outputpath;
			if(!hdfsmanager.exists(outputpath))
			{
				hdfsmanager.delete(new Path(outputpath),true);
			}
			hdfsmanager.getFileSystem().mkdirs(new Path(outputdir));
			FSDataOutputStream out = hdfsmanager.getFileSystem().create(new Path(outputpath));
			
			while (inputStream.available()>0)
			{
			    s = inputStream.readLine();	
			    StringTokenizer token =  new StringTokenizer(s," ,\n");
			    cnt2 = 0;
			    double []data = new double[this.col];
				while(token.hasMoreTokens())
				{
					data[cnt2] = Double.parseDouble(token.nextToken());
					
					cnt2++;
				}
				if(cnt1 == this.row-1)
					sourceDataToInputdata2(data,out,false);
				else
					sourceDataToInputdata2(data,out,true);
			    cnt1++;			    
			} 
			out.close();
			inputStream.close();
//			hdfsmanager.close();
			//sourceDataToInputdata(data);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return;
		}
	}

	@SuppressWarnings("deprecation")
	public int[][] setMiddlePointbySeperation(int K) {
		// TODO Auto-generated method stub
		int presetkmeans[][] = new int[Config.getInstance().col][K];
		//int krange = Config.getInstance().K_Range;
		int k_itr=0;
		int n = Config.getInstance().row;
		int m = Config.getInstance().col;
		int i,j,min,max;
		for(j=0;j<m;j++)
		{
			min = Integer.MAX_VALUE; 
			max = Integer.MIN_VALUE;
			for(i=0;i<n;i++)
			{
				if(min > data[i][j] )
					min = data[i][j];

				if(max < data[i][j] )
					max = data[i][j];
			}
			//System.out.println("set middle min/max : " + min + " " + max);
			for(k_itr=0;k_itr<K-1;k_itr++)
			{
				 presetkmeans[j][k_itr] = min + (max-min) / (K-1)* k_itr;
			}
			presetkmeans[j][k_itr] = max;
		}
		
		//default Centroid File Path kmean/Centroid/CentroidSepration_k_0    (k range value_ittr value)
		System.out.println("PresetKmean :: cetnroid with equally Sepration with k = " + K + "path = "
		+ Config.getInstance().CentroidDir + "clusters" + String.valueOf(K) + ".data");
		String outputpath = Config.getInstance().CentroidDir + "clusters" + String.valueOf(K) + ".data"; 
		Configuration conf = new Configuration();
		HDFSManager hdfsmanager;
		try {
			hdfsmanager = new HDFSManager(conf ,FileSystem.get(URI.create(outputpath),conf) );
			if(!hdfsmanager.exists(outputpath))
			{
				hdfsmanager.delete(new Path(outputpath),true);
			}
			FSDataInputStream outputstream;
			hdfsmanager.getFileSystem().mkdirs(new Path(Config.getInstance().CentroidDir));		
			FSDataOutputStream out = hdfsmanager.getFileSystem().create(new Path(outputpath));
			outputstream = hdfsmanager.getFileSystem().open(new Path(outputpath));
			for(k_itr=0;k_itr<K;k_itr++)
			{
				for(i=0;i<m;i++)
				{
					out.write(",".getBytes("UTF-8"));
					out.write(String.valueOf(presetkmeans[i][k_itr]).getBytes("UTF-8"));
										
					//out.write(" ");
				}
			//	out.write(String.valueOf(presetkmeans[i][k_itr]).getBytes("UTF-8"));
				out.write("\n".getBytes("UTF-8"));				
			}
			out.close();
//			hdfsmanager.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return presetkmeans;
	}
	public int[][] setMiddlePointbyRandom(int K)
	{
		int i,j;
		int presetkmeans[][] = new int[Config.getInstance().col][K];
		Map<Integer, String> map = new HashMap<Integer, String>();
		long n = Config.getInstance().row;
		int m = Config.getInstance().col;
		n = this.row;
		m = this.col;
		if(n<=K)
		{
			System.out.println(n+"( data row count ) must bigger than "+K);
			return null;
		}
			
		for(j=0;j<m;j++)
		{
			map.clear();
			for(i=0;i<K;i++)
			{
				int r = 0;
				while(true)
				{
					r = (int)Common.ComUtil.rand(n);
					if(!map.containsKey(r))
						break;
				}
				map.put(r, "");
				presetkmeans[j][i] = (int)data[r][j];
			}
		}
		//default Centroid File Path kmean/Centroid/CentroidRandom_k_0    (k range value_ittr value)
		HDFSManager hdfsmanager;
		String outputpath = Config.getInstance().CentroidDir + "clusters" + String.valueOf(K) + ".data";
		System.out.println("PresetKmean :: cetnroid with random with k = " + K + "path = "
				+ Config.getInstance().CentroidDir + "clusters" + String.valueOf(K) + ".data");
		Configuration conf = new Configuration();
		try {
			hdfsmanager = new HDFSManager(conf ,FileSystem.get(URI.create(outputpath),conf) );
			if(!hdfsmanager.exists(outputpath))
			{
				hdfsmanager.delete(new Path(outputpath),true);
			}
			FSDataInputStream outputstream;
			hdfsmanager.getFileSystem().mkdirs(new Path(Config.getInstance().CentroidDir));
			FSDataOutputStream out = hdfsmanager.getFileSystem().create(new Path(outputpath));
			outputstream = hdfsmanager.getFileSystem().open(new Path(outputpath));
			for(int k_itr=0;k_itr<K;k_itr++)
			{
				for(i=0;i<m;i++)
				{
					out.write(",".getBytes("UTF-8"));		
					out.write(String.valueOf(presetkmeans[i][k_itr]).getBytes("UTF-8"));
								
				}
			//	out.write(String.valueOf(presetkmeans[i][k_itr]).getBytes("UTF-8"));
				out.write("\n".getBytes("UTF-8"));				
			}
			out.close();
//			hdfsmanager.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			
			e.printStackTrace();
		}
		return presetkmeans;
	}
}
