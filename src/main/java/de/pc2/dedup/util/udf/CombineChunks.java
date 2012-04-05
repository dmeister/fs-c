package de.pc2.dedup.util.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.TupleFactory;
import java.security.MessageDigest;
import org.apache.commons.codec.binary.Base64;

public class CombineChunks extends EvalFunc<DataBag> {
    TupleFactory tupleFactory = TupleFactory.getInstance();
    BagFactory bagFactory = BagFactory.getInstance();

    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            Base64 base64 = new Base64();
            DataBag output = bagFactory.newDefaultBag();

            DataBag bag = (DataBag) input.get(0);   
            String lastObject = null;
            for(Object o : bag) {
                if (lastObject == null) {
                    lastObject = o.toString();
                } else {
                    md.update(base64.decodeBase64(lastObject));
                    md.update(base64.decodeBase64(o.toString()));
                    output.add(tupleFactory.newTuple(base64.encodeToString(md.digest())));
                    md.reset();
                    lastObject = null;
                }
            }
            if (lastObject != null) {
                output.add(tupleFactory.newTuple(lastObject));
            }
            return output;
        } catch(Exception e){
            throw WrappedIOException.wrap("Caught exception processing input row ", e);
        }
    }
}