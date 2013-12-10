package dima.kmeansseq;

import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.io.BinaryOutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Class write Named Vectors that were read from a SequenceFile
 * 
 * @author Michael Huelfenhaus
 */
public  class NamedVectorOutputFormat extends BinaryOutputFormat {

    private int counter = 0;

    @Override
    protected void serialize(PactRecord record, DataOutput out)
            throws IOException {
        PactString name = record.getField(0, PactString.class);
        PactVector nVector = record.getField(1, PactVector.class);
        name.write(out);
        nVector.write(out);

//        counter++;
//        System.out
//                .println("Out :" + counter + " " + name + " ||" + nVector);

    }

}