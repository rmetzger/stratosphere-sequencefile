package dima.kmeansseq;

//package eu.stratosphere.pact.example.iterative;

import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.generic.contract.BulkIteration;

/**
 *
 */
public class KmeansSeqIterative extends KmeansSeqFiles {

    /**
     * {@inheritDoc}
     */
    @Override
    public Plan getPlan(String... args) {
        // parse job parameters

        final int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
        final String dataPointInput = (args.length > 1 ? args[1] : "");
        // TODO add error handling
        final int dataCount = (args.length > 2 ? Integer.parseInt(args[2])
                : 100);
        final int kValue = (args.length > 3 ? Integer.parseInt(args[3]) : 10);
        final String outputPath = (args.length > 4 ? args[4] : "");
        final int numIterations = (args.length > 5 ? Integer.parseInt(args[5]) : 5);

        // read the vectors
        final FileDataSource dataPoints = new FileDataSource(
                SequenceFileInputFormat.class, dataPointInput, "Input");
        // set classes for key and value
        SequenceFileInputFormat.configureSequenceFormat(dataPoints).setClasses(PactString.class,PactVector.class);
        //dataPoints.setDegreeOfParallelism(1);
        
//        // create DataSourceContract for cluster center input
//        FileDataSource initialClusterPoints = new FileDataSource(
//                PointInFormat.class, clusterInput, "Centers");
//        initialClusterPoints.setParameter(
//                DelimitedInputFormat.RECORD_DELIMITER, "\n");
//        initialClusterPoints.setDegreeOfParallelism(1);
//        initialClusterPoints.getCompilerHints().setUniqueField(new FieldSet(0));

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

        // create iteration and set number of iterations
        BulkIteration iteration = new BulkIteration("K-Means Loop");
        iteration.setInput(initialClusterPoints);
        iteration.setNumberOfIterations(numIterations);

        // create CrossContract for distance computation
        CrossContract computeDistance = CrossContract
                .builder(ComputeDistance.class).input1(dataPoints)
                .input2(iteration.getPartialSolution())
                .name("Compute Distances").build();
        computeDistance.getCompilerHints().setAvgBytesPerRecord(48);

        // create ReduceContract for finding the nearest cluster centers
        ReduceContract findNearestClusterCenters = ReduceContract
                .builder(FindNearestCenter.class, PactString.class, 0)
                .input(computeDistance).name("Find Nearest Centers").build();
        findNearestClusterCenters.getCompilerHints().setAvgBytesPerRecord(48);

        // create ReduceContract for computing new cluster positions
        ReduceContract recomputeClusterCenter = ReduceContract
                .builder(RecomputeClusterCenter.class, PactString.class, 0)
                .input(findNearestClusterCenters)
                .name("Recompute Center Positions").build();
        recomputeClusterCenter.getCompilerHints().setAvgBytesPerRecord(36);
        iteration.setNextPartialSolution(recomputeClusterCenter);

        // create DataSinkContract for writing the new cluster positions
        FileDataSink finalResult = new FileDataSink(
                NamedVectorOutputFormat.class, outputPath, iteration,
                "New Center Positions");

        // return the PACT plan
        Plan plan = new Plan(finalResult, "Iterative KMeans with Sequence Files");
        plan.setDefaultParallelism(noSubTasks);
        return plan;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * eu.stratosphere.pact.example.datamining.KMeansIteration#getDescription()
     */
    @Override
    public String getDescription() {
        return "Parameters: <noSubStasks> <dataPoints> <clusterCenters> <output> <numIterations>";
    }
}
