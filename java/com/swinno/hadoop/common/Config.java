package com.swinno.hadoop.common;
import java.util.HashMap;
import java.util.Map;

/* config value list //
 * sangil - 2018/01/30
 * default String int/double should be convert to int/double 
 *   key    value       default
 *  K_Range  int
 *  SourcePath String
 *  InputPath String
 *  OutputPath String
 *  CentroidPath String
 *  ErrorPath String
 *  Dimension int
 *  MaxItr  int
 *  ConvDelta double (%)
 *  ConfPath String   kmean/conf
 */
public class Config {
	//static singleton class config	 
	private static Config config = null;
	//private Map<String,String> configmap;
	public int K_RangeStart,K_RangeEnd;
	public String InputDir,OutputDir,ConfPath,SourcePath,CentroidDir,ErrorDir;
	public String InputFirst_FilePath,SourceFilePath;
	public int Dimension,MaxItr;
	public double ConvDelta;
	public int row,col;
	public Config()
	{
		// TODO set all default value of config file here
		//configmap = new HashMap<String, String>();
		//configmap.clear();
		ConfPath = "data/kmeans/conf";
		ErrorDir = "data/kmeans/error/";
		CentroidDir = "data/kmeans/";
		SourcePath = "data/kmeans/in/source.data";
		InputDir = "data/kmeans-input/";
		InputFirst_FilePath = "data/kmeans-input/input";
		OutputDir = "data/kmeans-output/";
		
	}
	public static Config getInstance( )
	{
		 if(config == null)
			 config = new Config();
	      return config;
    }
	public static Config initInstance( ) 
	{
		 if(config == null)
			 config = new Config();
		 else 
		 {
			 config = null;
			 config = new Config();
		 }
	      return config;
   }
	public void setValue(String key,String value)
	{
//		System.out.println("Config :: setValue >> "+ key +" : " + value);
		key = key.toLowerCase();
		switch(key)
		{
	//	case "InputPath":  InputPath = (value.charAt(value.length()-1) == '/')?value : (value +'/'); break;
//		case "inputpath": case "inputdir":  InputDir = (value.charAt(value.length()-1) == '/')?value : (value +'/'); break;
		case "inputpath":
		case "inputdir":  InputDir = value; break;
	//	case "OutputPath": OutputPath = (value.charAt(value.length()-1) == '/')?value : (value +'/'); break;
		case "outputpath": case "outputdir":  OutputDir = (value.charAt(value.length()-1) == '/')?value : (value +'/'); break;
	//	case "ConfPath": ConfPath = (value.charAt(value.length()-1) == '/')?value : (value +'/'); break;
		case "confpath":  ConfPath = value; break;
		case "centroidpath": case "centroiddir": CentroidDir = (value.charAt(value.length()-1) == '/')?value : (value +'/'); break;
		case "errorpath": case "Errordir": ErrorDir = (value.charAt(value.length()-1) == '/')?value : (value +'/'); break;
		case "sourcepath": SourcePath = (value.charAt(value.length()-1) == '/')?value : (value +'/'); break;
		case "k_range": K_RangeStart = Integer.parseInt(value); K_RangeEnd = Integer.parseInt(value); break;
		case "dimension": Dimension = Integer.parseInt(value); break;
		case "maxitr": MaxItr = Integer.parseInt(value); break;
		case "convdelta": ConvDelta = Double.parseDouble(value); break;
		default: break;
		}
	//	configmap.put(key, value);		
	}
	public void setValue(String key,String value, String value2)
	{
		key = key.toLowerCase();
		switch(key)
		{
		case "k_range":  K_RangeStart = Integer.parseInt(value); K_RangeEnd = Integer.parseInt(value2); break;
		default: break;
		}
	}
	public void setValue(String key,int value)
	{
		key = key.toLowerCase();
		switch(key)
		{
		case "k_rangestart": K_RangeStart = value; break;
		case "k_rangeend": K_RangeEnd = value; break;
		case "dimension": Dimension = value; break;
		case "maxitr": MaxItr = value; break;
		case "row": row = value; break;
		case "col": col = value; break;
		default: break;
		}
		//configmap.put(key, value);		
	}
	public void setValue(String key,double value)
	{
		switch(key)
		{
		case "ConvDelta": ConvDelta = value; break;
		default: break;
		}
		//configmap.put(key, value);		
	}
	public String getValueStr(String key)
	{
		switch(key)
		{
		case "InputDir": return InputDir;
		case "OutputDir": return OutputDir;  
		case "ConfPath": return ConfPath;
		default: break;
		}
		return "";
	}
	public int getValueInt(String key)
	{
		switch(key)
		{
		case "K_RangeStart": return K_RangeStart;
		case "K_RangeEnd": return K_RangeEnd;
		case "Dimension": return Dimension;
		case "MaxItr": return MaxItr;
		default: break;
		}
		return 0;
	}
	public double getValueDouble(String key)
	{
		switch(key)
		{
		case "ConvDelta": return ConvDelta;
		default: break;
		}
		return 0;
	}
}
