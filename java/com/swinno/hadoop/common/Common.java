package com.swinno.hadoop.common;

import java.util.Random;

public class Common {
	public static class ComUtil
	{
		public static void printLog(String a)
		{
			System.out.println("outt!!");
		}
		public static long rand(long m)
		{
			Random random = new Random();
			long a = random.nextLong()%(m);
	    	if(a<0) a+= m;
	    	return a;
		}
		
	}
	
}
