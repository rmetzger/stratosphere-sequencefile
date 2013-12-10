package dima.kmeansseq;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;


import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.DelimitedOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirstExcept;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.util.FieldSet;

/**
 * The K-Means cluster algorithm is well-known (see
 * http://en.wikipedia.org/wiki/K-means_clustering). KMeansIteration is a PACT
 * program that computes a single iteration of the k-means algorithm. The job
 * has two inputs, a set of data points and a set of cluster centers. A Cross
 * PACT is used to compute all distances from all centers to all points. A
 * following Reduce PACT assigns each data point to the cluster center that is
 * next to it. Finally, a second Reduce PACT compute the new locations of all
 * cluster centers.
 * 
 * @author Fabian Hueske
 */
public class KmeansSeqFiles implements PlanAssembler {

    // /**
    // * Generates records with an id and a and CoordVector.
    // * The input format is line-based, i.e. one record is read from one line
    // * which is terminated by '\n'. Within a line the first '|' character
    // separates
    // * the id from the the CoordVector. The vector consists of a vector of
    // decimals.
    // * The decimals are separated by '|' as well. The id is the id of a data
    // point or
    // * cluster center and the CoordVector the corresponding position
    // (coordinate
    // * vector) of the data point or cluster center. Example line:
    // * "42|23.23|52.57|74.43| Id: 42 Coordinate vector: (23.23, 52.57, 74.43)
    // *
    // * @author Fabian Hueske
    // */
    // public static class PointInFormat extends DelimitedInputFormat
    // {
    // private final PactInteger idInteger = new PactInteger();
    // Vector v = new ;
    // private final CoordVector point = new CoordVector();
    //
    // private final List<Double> dimensionValues = new ArrayList<Double>();
    // private double[] pointValues = new double[0];
    //
    // @Override
    // public boolean readRecord(PactRecord record, byte[] line, int offset, int
    // numBytes)
    // {
    // final int limit = offset + numBytes;
    //
    // int id = -1;
    // int value = 0;
    // int fractionValue = 0;
    // int fractionChars = 0;
    //
    // this.dimensionValues.clear();
    //
    // for (int pos = offset; pos < limit; pos++) {
    // if (line[pos] == '|') {
    // // check if id was already set
    // if (id == -1) {
    // id = value;
    // }
    // else {
    // this.dimensionValues.add(value + ((double) fractionValue) * Math.pow(10,
    // (-1 * (fractionChars - 1))));
    // }
    // // reset value
    // value = 0;
    // fractionValue = 0;
    // fractionChars = 0;
    // } else if (line[pos] == '.') {
    // fractionChars = 1;
    // } else {
    // if (fractionChars == 0) {
    // value *= 10;
    // value += line[pos] - '0';
    // } else {
    // fractionValue *= 10;
    // fractionValue += line[pos] - '0';
    // fractionChars++;
    // }
    // }
    // }
    //
    // // set the ID
    // this.idInteger.setValue(id);
    // record.setField(0, this.idInteger);
    //
    // // set the data points
    // if (this.pointValues.length != this.dimensionValues.size()) {
    // this.pointValues = new double[this.dimensionValues.size()];
    // }
    // for (int i = 0; i < this.pointValues.length; i++) {
    // this.pointValues[i] = this.dimensionValues.get(i);
    // }
    //
    // this.point.setCoordinates(this.pointValues);
    // record.setField(1, this.point);
    // return true;
    // }
    // }

    // /**
    // * Writes records that contain an id and a CoordVector.
    // * The output format is line-based, i.e. one record is written to
    // * a line and terminated with '\n'. Within a line the first '|' character
    // * separates the id from the CoordVector. The vector consists of a vector
    // of
    // * decimals. The decimals are separated by '|'. The is is the id of a data
    // * point or cluster center and the vector the corresponding position
    // * (coordinate vector) of the data point or cluster center. Example line:
    // * "42|23.23|52.57|74.43| Id: 42 Coordinate vector: (23.23, 52.57, 74.43)
    // *
    // * @author Michael Hülfenhaus
    // * @author Fabian Hueske
    // *
    // */
    // public static class PointOutFormat extends DelimitedOutputFormat {
    // private final DecimalFormat df = new DecimalFormat("####0.00");
    // private final StringBuilder line = new StringBuilder();
    //
    // public PointOutFormat() {
    // DecimalFormatSymbols dfSymbols = new DecimalFormatSymbols();
    // dfSymbols.setDecimalSeparator('.');
    // this.df.setDecimalFormatSymbols(dfSymbols);
    // }
    //
    // @Override
    // public int serializeRecord(PactRecord record, byte[] target) {
    // line.setLength(0);
    //
    // PactInteger centerId = record.getField(0, PactInteger.class);
    // // CoordVector centerPos = record.getField(1, CoordVector.class);
    //
    // line.append(centerId.getValue());
    //
    // // for (double coord : centerPos.getCoordinates()) {
    // // line.append('|');
    // // line.append(df.format(coord));
    // // }
    // line.append('|');
    //
    // byte[] byteString = line.toString().getBytes();
    //
    // if (byteString.length <= target.length) {
    // System.arraycopy(byteString, 0, target, 0, byteString.length);
    // return byteString.length;
    // } else {
    // return -byteString.length;
    // }
    // }
    // }

    /**
     * Choose records randomly by their position in the stream
     * 
     * @author mhuelfen
     * 
     */
    public static class ChooseRandomPoints extends MapStub {

        public static final String RANDOM_COUNT = "parameter.RANDOM_COUNT";
        public static final String DATA_COUNT = "parameter.DATA_COUNT";

        private int dataCount = -1;
        private int randomCount = -1;
        private Set<Integer> randomIds;
        private int id = 0;

        /*
         * (non-Javadoc)
         * 
         * @see
         * eu.stratosphere.pact.common.io.DelimitedInputFormat#configure(eu.
         * stratosphere.nephele.configuration.Configuration)
         */
        @Override
        public void open(Configuration parameters) {
            // Read number of data points to get the range for the random ints
            this.dataCount = parameters.getInteger(DATA_COUNT, -1);

            // Read number of random ints to generate
            this.randomCount = parameters.getInteger(RANDOM_COUNT, -1);

            // choose random Ids
            randomIds = new HashSet<Integer>();

            // generate the random int ids
            Random randomGenerator = new Random();
            while (randomIds.size() <= randomCount) {
                int randomInt = randomGenerator.nextInt(dataCount);
                randomIds.add(randomInt);
            }
        }

        /**
         * Choose records if their positions were randomly chosen in the open
         * method
         */
        @Override
        public void map(PactRecord record, Collector<PactRecord> out)
                throws Exception {

            // check if this point was choosen at random
            if (randomIds.contains(id)) {
                out.collect(record);
            }
            id++;
        }
    }

    
    /**
     * Cross PACT computes the distance of all data points to all cluster
     * centers.
     * <p>
     * 
     * @author Fabian Hueske
     */
// TODO tel about strange bug
    //    @ConstantFieldsFirstExcept(fields = { 2, 3 })
    @OutCardBounds(lowerBound = 1, upperBound = 1)
    public static class ComputeDistance extends CrossStub {
        private final PactDouble distance = new PactDouble();

        /**
         * Computes the distance of one data point to one cluster center.
         * 
         * Output Format:
         * 0: pointID
         * 1: pointVector
         * 2: clusterID
         * 3: distance
         */
        @Override
        public void cross(PactRecord dataPointRecord,
                PactRecord clusterCenterRecord, Collector<PactRecord> out) {

            PactString str = dataPointRecord.getField(0, PactString.class);
//           System.out.println(str);
//            dataPointRecord.setField(0, dataPointRecord.getField(0, PactString.class));
            
            this.distance.setValue(dataPointRecord
                    .getField(1, PactVector.class)
                    .computeEuclidianDistance(clusterCenterRecord.getField(1,
                            PactVector.class)));

            // add cluster center id and distance to the data point record
            
            dataPointRecord.setField(2, clusterCenterRecord.getField(0,
                    PactString.class));
            dataPointRecord.setField(3, this.distance);
            

//            System.out.println("dis " + distance.toString() );
            out.collect(dataPointRecord);
        }
    }

    /**
     * Reduce PACT determines the closes cluster center for a data point. This
     * is a minimum aggregation. Hence, a Combiner can be easily implemented.
     * 
     * @author Fabian Hueske
     */
    @ConstantFields(fields = { 1 })
    @OutCardBounds(lowerBound = 1, upperBound = 1)
    @Combinable
    public static class FindNearestCenter extends ReduceStub {
        private final PactString centerId = new PactString();
        private final PactVector position = new PactVector();
        private final PactInteger one = new PactInteger(1);

        private final PactRecord result = new PactRecord(3);

        /**
         * Computes a minimum aggregation on the distance of a data point to
         * cluster centers.
         * 
         * Output Format:
         * 0: centerID
         * 1: pointVector
         * 2: constant(1) (to enable combinable average computation in the
         * following reducer)
         */
        @Override
        public void reduce(Iterator<PactRecord> pointsWithDistance,
                Collector<PactRecord> out) {
            double nearestDistance = Double.MAX_VALUE;
            // int nearestClusterId = 0;
            String nearestClusterId = "";

            // check all cluster centers
            while (pointsWithDistance.hasNext()) {
                PactRecord res = pointsWithDistance.next();

                double distance = res.getField(3, PactDouble.class).getValue();

                // compare distances
                if (distance < nearestDistance) {
                    // if distance is smaller than smallest till now, update
                    // nearest cluster
                    nearestDistance = distance;
                    // nearestClusterId = res.getField(2, PactInteger.class)
                    // .getValue();
                    nearestClusterId = res.getField(2, PactString.class)
                            .getValue();

                    res.getFieldInto(1, this.position);
                }
            }

            // emit a new record with the center id and the data point. add a
            // one to ease the
            // implementation of the average function with a combiner
            this.centerId.setValue(nearestClusterId);
            this.result.setField(0, this.centerId);
            this.result.setField(1, this.position);
            this.result.setField(2, this.one);

            out.collect(this.result);
        }

        // ----------------------------------------------------------------------------------------

        private final PactRecord nearest = new PactRecord();

        /**
         * Computes a minimum aggregation on the distance of a data point to
         * cluster centers.
         */
        @Override
        public void combine(Iterator<PactRecord> pointsWithDistance,
                Collector<PactRecord> out) {
            double nearestDistance = Double.MAX_VALUE;

            // check all cluster centers
            while (pointsWithDistance.hasNext()) {
                PactRecord res = pointsWithDistance.next();
                double distance = res.getField(3, PactDouble.class).getValue();

                // compare distances
                if (distance < nearestDistance) {
                    nearestDistance = distance;
                    res.copyTo(this.nearest);
                }
            }

            // emit nearest one
            out.collect(this.nearest);
        }
    }

    /**
     * Reduce PACT computes the new position (coordinate vector) of a cluster
     * center. This is an average computation. Hence, Combinable is annotated
     * and the combine method implemented.
     * 
     * Output Format:
     * 0: clusterID
     * 1: clusterVector
     * 
     * @author Fabian Hueske
     */

    @ConstantFields(fields = { 0 })
    @OutCardBounds(lowerBound = 1, upperBound = 1)
    @Combinable
    public static class RecomputeClusterCenter extends ReduceStub {
        private final PactInteger count = new PactInteger();

        // private final
        private final PactVector centerVector = new PactVector();

        /**
         * Compute the new position (coordinate vector) of a cluster center.
         */
        @Override
        public void reduce(Iterator<PactRecord> dataPoints,
                Collector<PactRecord> out) {
            PactRecord next = null;

            // initialize coordinate vector sum and count
            // PactVector coordinates = new PactVector();
            // double[] coordinateSum = null;
            Vector sumVector = null;
            int count = 0;

            // compute coordinate vector sum and count
            while (dataPoints.hasNext()) {
                next = dataPoints.next();

                // get the coordinates and the count from the record
                Vector thisVector = next.getField(1, PactVector.class)
                        .getVector();
                int thisCount = next.getField(2, PactInteger.class).getValue();

                // add Vectors
                if (sumVector == null) {
                    sumVector = thisVector;
                } else {
                    sumVector.plus(thisVector);
                }

                count += thisCount;
            }

            // compute new coordinate vector (position) of cluster center
            sumVector.divide(count);

            centerVector.setVector(sumVector);

            next.setField(1, centerVector);
            next.setNull(2);

            // emit new position of cluster center
            out.collect(next);
        }

        /**
         * Computes a pre-aggregated average value of a coordinate vector.
         */
        @Override
        public void combine(Iterator<PactRecord> dataPoints,
                Collector<PactRecord> out) {
            PactRecord next = null;

            // initialize coordinate vector sum and count
            Vector sumVector = null;
            int count = 0;

            // compute coordinate vector sum and count
            while (dataPoints.hasNext()) {
                next = dataPoints.next();

                // get the coordinates and the count from the record
                Vector thisVector = next.getField(1, PactVector.class)
                        .getVector();

                int thisCount = next.getField(2, PactInteger.class).getValue();

                // add Vectors
                if (sumVector == null) {
                    sumVector = thisVector;
                } else {
                    sumVector.plus(thisVector);
                }

                count += thisCount;
            }

            centerVector.setVector(sumVector);
            this.count.setValue(count);

            next.setField(1, centerVector);
            next.setField(2, this.count);

            // emit partial sum and partial count for average computation
            out.collect(next);
        }

        /**
         * Adds two coordinate vectors by summing up each of their coordinates.
         * 
         * @param cvToAddTo
         *            The coordinate vector to which the other vector is added.
         *            This vector is returned.
         * @param cvToBeAdded
         *            The coordinate vector which is added to the other vector.
         *            This vector is not modified.
         */
        private void addToCoordVector(double[] cvToAddTo, double[] cvToBeAdded) {

            // check if both vectors have same length
            if (cvToAddTo.length != cvToBeAdded.length) {
                throw new IllegalArgumentException(
                        "The given coordinate vectors are not of equal length.");
            }

            // sum coordinate vectors coordinate-wise
            for (int i = 0; i < cvToAddTo.length; i++) {
                cvToAddTo[i] += cvToBeAdded[i];
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    // @Override
    public Plan getPlan(String... args) {
        // parse job parameters
        int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
        String dataPointInput = (args.length > 1 ? args[1] : "");
        // TODO add error handling
        int dataCount = (args.length > 2 ? Integer.parseInt(args[2]) : 100);
        int kValue = (args.length > 3 ? Integer.parseInt(args[3]) : 10);
        String outputPath = (args.length > 4 ? args[4] : "");

        final FileDataSource dataPoints = new FileDataSource(
                //NamedVectorInputFormat.class,
                SequenceFileInputFormat.class,
                dataPointInput,"Input");
//                
        // set classes for key and value
        SequenceFileInputFormat.configureSequenceFormat(dataPoints).setClasses(PactString.class,PactVector.class);

        // select initial cluster centers at random
        MapContract initialClusterPoints = MapContract
                .builder(ChooseRandomPoints.class).input(dataPoints)
                .name("choose Centers").build();
        // important because random process only works with one mapper
        initialClusterPoints.setDegreeOfParallelism(1);    
        // set parameters for choosing points as initial cluster centers at
        // random
        initialClusterPoints.setParameter(ChooseRandomPoints.DATA_COUNT,
                dataCount);
        // Random count = k value of K Means
        initialClusterPoints.setParameter(ChooseRandomPoints.RANDOM_COUNT,
                kValue);

        // create CrossContract for distance computation
        CrossContract computeDistance = CrossContract
                .builder(ComputeDistance.class).input1(dataPoints)
                .input2(initialClusterPoints).name("Compute Distances").build();
        computeDistance.getCompilerHints().setAvgBytesPerRecord(48);

        // create ReduceContract for finding the nearest cluster centers
        ReduceContract findNearestClusterCenters = new ReduceContract.Builder(
                FindNearestCenter.class, PactString.class, 0)
                .input(computeDistance).name("Find Nearest Centers").build();
        findNearestClusterCenters.getCompilerHints().setAvgBytesPerRecord(48);

        // create ReduceContract for computing new cluster positions
        ReduceContract recomputeClusterCenter = new ReduceContract.Builder(
                RecomputeClusterCenter.class, PactString.class, 0)
                .input(findNearestClusterCenters)
                .name("Recompute Center Positions").build();
        recomputeClusterCenter.getCompilerHints().setAvgBytesPerRecord(36);
        
        // create DataSinkContract for writing the new cluster positions
        FileDataSink newClusterPoints = new FileDataSink(
                NamedVectorOutputFormat.class, outputPath, recomputeClusterCenter,
                "New Center Positions");
        // return the PACT plan
        Plan plan = new Plan(newClusterPoints, "KMeans Iteration");
        plan.setDefaultParallelism(noSubTasks);
        return plan;
    }

    // @Override
    public String getDescription() {
        return "Parameters: [noSubStasks] [dataPoints] [clusterCenters] [tupleCount] [k-Value] [output]";
    }

}
