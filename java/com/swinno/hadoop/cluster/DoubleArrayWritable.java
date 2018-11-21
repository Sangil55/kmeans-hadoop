package com.swinno.hadoop.cluster;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class DoubleArrayWritable extends ArrayWritable implements WritableComparable {
	
	private static final long serialVersionUID = 1L;
	
	public DoubleArrayWritable()
	{
		super(DoubleWritable.class);
	}
	
	public DoubleArrayWritable(DoubleWritable[] values)
	{
		super(DoubleWritable.class, values);
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		DoubleArrayWritable input = (DoubleArrayWritable)o;
		double inputValue = ((DoubleWritable)input.get()[0]).get() + ((DoubleWritable)input.get()[1]).get();
		double ownValue = ((DoubleWritable)this.get()[0]).get() + ((DoubleWritable)this.get()[1]).get();
		
		
		return inputValue > ownValue ? 1:0;
	}
	
	/*
	public DoubleWritable[] get()
	{
		return (DoubleWritable[]) super.get();
	}
	*/
	/*
	public String toString()
	{
		DoubleWritable[] values = get();
		String str = "";
		int count = 0;
		
		for(DoubleWritable value : values)
		{
			str = str + value.toString();
			
			if(count != values.length)
				str = str+",";
			
			count++;
			
		}
		return str;
	}
	*/
}
