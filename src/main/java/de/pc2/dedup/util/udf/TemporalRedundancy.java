package de.pc2.dedup.util.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class TemporalRedundancy extends EvalFunc<Tuple> {

	@Override
	public Tuple exec(Tuple t) throws IOException {
		if(t == null || t.size() != 3) {
			return null;
		}
		Long oldOccurs = (Long) t.get(0);
		Long newOccurs = (Long) t.get(1);
		Double chunkSize = (Double) t.get(2);

		double realSize = chunkSize;
		if(oldOccurs != null && oldOccurs > 0) {
			realSize = 0;
		}
		double totalSize = newOccurs * chunkSize;
		Tuple result = TupleFactory.getInstance().newTuple(2);
		result.set(0, totalSize);
		result.set(1, realSize);
		return result;
	}
	public Schema outputSchema(Schema input) {
		try{
			Schema tupleSchema = new Schema();
			tupleSchema.add(new Schema.FieldSchema("totalsize", DataType.DOUBLE));
			tupleSchema.add(new Schema.FieldSchema("realsize", DataType.DOUBLE));
			return tupleSchema;
		}catch (Exception e){
			return null;
		}
	}
}
