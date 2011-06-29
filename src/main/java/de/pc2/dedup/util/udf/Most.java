package de.pc2.dedup.util.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

public class Most extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
            return null;
        try{
        	DataBag bag = (DataBag) input.get(0);
        	CountingMap<String> map = new CountingMap<String>();
		
        	for(Object o : bag) {
        		String item = o.toString();
        		map.add(item);
			}
			return map.getMax();
        }catch(Exception e){
            throw WrappedIOException.wrap("Caught exception processing input row ", e);
        }
	}
}
