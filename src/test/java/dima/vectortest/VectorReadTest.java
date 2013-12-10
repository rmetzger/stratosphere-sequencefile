package dima.vectortest;


import java.util.Collection;
import java.util.Collections;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import dima.kmeansseq.NamedVectorOutputFormat;
import dima.kmeansseq.PactVector;
import dima.kmeansseq.SequenceFileInputFormat;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;
//import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
//import eu.stratosphere.pact.compiler.plan.OptimizedPlan;

import eu.stratosphere.pact.test.util.TestBase;


@RunWith(Parameterized.class)
public class VectorReadTest extends TestBase {

    
    public VectorReadTest(Configuration config) {
        super(config);
    }

    @Override
    protected JobGraph getJobGraph() throws Exception {

        final FileDataSource source = new FileDataSource(
                //NamedVectorInputFormat.class,
                SequenceFileInputFormat.class,
//                "hdfs://localhost:9000/user/mhuelfen/input/small_one_file","Input");
                "hdfs://localhost:9000/user/mhuelfen/input/vectors","Input");
//                "hdfs://localhost:9000/user/mhuelfen/input/small__vectors","Input");
                 
        // set classes for key and value
        SequenceFileInputFormat.configureSequenceFormat(source).setClasses(PactString.class,PactVector.class);

        FileDataSink output = new FileDataSink(NamedVectorOutputFormat.class,
                "hdfs://localhost:9000/user/mhuelfen/output","Output");
        output.setInput(source);

        Plan testPlan = new Plan(output);
        testPlan.setDefaultParallelism(1);       

        PactCompiler pc = new PactCompiler();
        OptimizedPlan op = pc.compile(testPlan);
        NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
        return jgg.compileJobGraph(op);
    }

    @Override
    protected void preSubmit() throws Exception {
    }

    @Override
    protected void postSubmit() throws Exception {
    }
    
    @Parameters
    public static Collection<Object[]> getConfigurations() {
        return toParameterList(Collections.singletonList(new Configuration()));
    }
    
}
