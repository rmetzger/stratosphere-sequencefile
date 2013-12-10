package dima.kmeansseq;

import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.io.BinaryOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Hello world!
 * 
 */
public class ReadingMahoutVectors implements PlanAssembler,
        PlanAssemblerDescription {

    public String getDescription() {
        return "Parameters: [noSubStasks] [sourceFile] [Targetfolder]";

    }

    public Plan getPlan(String... args) {
        // parse job parameters
        int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
        String input = (args.length > 1 ? args[1] : "");
        String output = (args.length > 2 ? args[2] : "");
        // create DataSourceContract for data point input
        FileDataSource source = new FileDataSource(
                SequenceFileInputFormat.class, input, "Sequence File");
        // set classes for reading the sequence file
        SequenceFileInputFormat.configureSequenceFormat(source).setClasses(
                PactString.class, PactVector.class);

        // create DataSinkContract for writing the new cluster positions
        FileDataSink outputTarget = new FileDataSink(
                NamedVectorOutputFormat.class, output, source,
                "New Center Positions");

        // return the PACT plan
        Plan plan = new Plan(outputTarget, "Reading SequenceFiles");
        plan.setDefaultParallelism(noSubTasks);
        return plan;
    }
}
