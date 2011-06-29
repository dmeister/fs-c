package de.pc2.dedup.util.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

public class FileSizeCategory extends EvalFunc<Long> {
	private static double LOG_2 = Math.log(2);
	
	private long getCategory(double fileSize) {
		double l = log2(fileSize);
		return (long) Math.pow(2,Math.floor(l));
	}
	
	private double log2(double value ) {
		return Math.log(value) / LOG_2;
	}
	
	@Override
	public Long exec(Tuple input) throws IOException {
		 if (input == null || input.size() == 0)
	            return null;
	        try{
	        	double fileSize = Double.parseDouble(input.get(0).toString());
	            return getCategory(fileSize);
	        }catch(Exception e){
	            throw WrappedIOException.wrap("Caught exception processing input row ", e);
	        }
	}
}
