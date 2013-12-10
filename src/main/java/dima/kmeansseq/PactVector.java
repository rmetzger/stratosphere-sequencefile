package dima.kmeansseq;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Varint;
import org.apache.mahout.math.Vector;

import com.google.common.base.Preconditions;

import eu.stratosphere.pact.common.type.Key;

public class PactVector implements Key {

    public static final int FLAG_DENSE = 0x01;
    public static final int FLAG_SEQUENTIAL = 0x02;
    public static final int FLAG_NAMED = 0x04;
    public static final int FLAG_LAX_PRECISION = 0x08;
    public static final int NUM_FLAGS = 4;

    private Vector vector;

    public Vector getVector() {
        return vector;
    }

    public void setVector(Vector vector) {
        this.vector = vector;
    }

    public void read(DataInput in) throws IOException {
        int flags = in.readByte();
        Preconditions.checkArgument(flags >> NUM_FLAGS == 0,
                "Unknown flags set: %d", Integer.toString(flags, 2));
        boolean dense = (flags & FLAG_DENSE) != 0;
        boolean sequential = (flags & FLAG_SEQUENTIAL) != 0;
        boolean named = (flags & FLAG_NAMED) != 0;
        boolean laxPrecision = (flags & FLAG_LAX_PRECISION) != 0;

        int size = Varint.readUnsignedVarInt(in);
        Vector v;
        if (dense) {
            double[] values = new double[size];
            for (int i = 0; i < size; i++) {
                values[i] = laxPrecision ? in.readFloat() : in.readDouble();
            }
            v = new DenseVector(values);
        } else {
            int numNonDefaultElements = Varint.readUnsignedVarInt(in);
            v = sequential ? new SequentialAccessSparseVector(size,
                    numNonDefaultElements) : new RandomAccessSparseVector(size,
                    numNonDefaultElements);
            if (sequential) {
                int lastIndex = 0;
                for (int i = 0; i < numNonDefaultElements; i++) {
                    int delta = Varint.readUnsignedVarInt(in);
                    int index = lastIndex + delta;
                    lastIndex = index;
                    double value = laxPrecision ? in.readFloat() : in
                            .readDouble();
                    v.setQuick(index, value);
                }
            } else {
                for (int i = 0; i < numNonDefaultElements; i++) {
                    int index = Varint.readUnsignedVarInt(in);
                    double value = laxPrecision ? in.readFloat() : in
                            .readDouble();
                    v.setQuick(index, value);
                }
            }
        }
        if (named) {
            String name = in.readUTF();
            v = new NamedVector(v, name);
        }
        vector = v;
//        v.g
    }

    public void write(DataOutput out) throws IOException {
        boolean laxPrecision = false;

        boolean dense = vector.isDense();
        boolean sequential = vector.isSequentialAccess();
        boolean named = vector instanceof NamedVector;

        out.writeByte((dense ? FLAG_DENSE : 0)
                | (sequential ? FLAG_SEQUENTIAL : 0) | (named ? FLAG_NAMED : 0)
                | (laxPrecision ? FLAG_LAX_PRECISION : 0));

        Varint.writeUnsignedVarInt(vector.size(), out);
        if (dense) {
            for (Vector.Element element : vector) {
                if (laxPrecision) {
                    out.writeFloat((float) element.get());
                } else {
                    out.writeDouble(element.get());
                }
            }
        } else {
            Varint.writeUnsignedVarInt(vector.getNumNondefaultElements(), out);
            Iterator<Vector.Element> iter = vector.iterateNonZero();
            if (sequential) {
                int lastIndex = 0;
                while (iter.hasNext()) {
                    Vector.Element element = iter.next();
                    int thisIndex = element.index();
                    // Delta-code indices:
                    Varint.writeUnsignedVarInt(thisIndex - lastIndex, out);
                    lastIndex = thisIndex;
                    if (laxPrecision) {
                        out.writeFloat((float) element.get());
                    } else {
                        out.writeDouble(element.get());
                    }
                }
            } else {
                while (iter.hasNext()) {
                    Vector.Element element = iter.next();
                    Varint.writeUnsignedVarInt(element.index(), out);
                    if (laxPrecision) {
                        out.writeFloat((float) element.get());
                    } else {
                        out.writeDouble(element.get());
                    }
                }
            }
        }
        if (named) {
            String name = ((NamedVector) vector).getName();
            out.writeUTF(name == null ? "" : name);
        }
    }
    
    /**
     * Computes the Euclidian distance between this coordinate vector and a
     * second coordinate vector.
     * 
     * @param cv The coordinate vector to which the distance is computed.
     * @return The Euclidian distance to coordinate vector cv. If cv has a
     *         different length than this coordinate vector, -1 is returned.
     */
    public double computeEuclidianDistance(PactVector otherVector)
    {
        // check coordinate vector lengths
        if (otherVector.vector.size() != this.vector.size()) {
            return -1.0;
        }

        double quadSum = 0.0;
        for (int i = 0; i < this.vector.size(); i++) {
            double diff = this.vector.get(i) - otherVector.vector.get(i);
            quadSum += diff*diff;
        }
        return Math.sqrt(quadSum);
    }
    

    @Override
    public String toString() {
        return vector.toString();
    }

    public int compareTo(Key o) {
        // check if other key is also of type CoordVector
        if (!(o instanceof PactVector)) {
            return -1;
        }
        // cast to CoordVector
        PactVector oP = (PactVector) o;

        // check if both coordinate vectors have identical lengths
        if (oP.vector.size() > this.vector.size()) {
            return -1;
        }
        else if (oP.vector.size() < this.vector.size()) {
            return 1;
        }

        // compare all coordinates
        for (int i = 0; i < this.vector.size(); i++) {
            if (oP.vector.get(i) > this.vector.get(i)) {
                return -1;
            } else if (oP.vector.get(i) < this.vector.get(i)) {
                return 1;
            }
        }
        return 0;
    }

}
