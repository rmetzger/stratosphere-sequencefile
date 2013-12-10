package dima.vectortest;

import java.util.Collection;
import java.util.Collections;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import dima.kmeansseq.KmeansSeqFiles;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;
//import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
//import eu.stratosphere.pact.compiler.plan.OptimizedPlan;

import eu.stratosphere.pact.test.util.TestBase;

@RunWith(Parameterized.class)
public class KMeansTest extends TestBase {



    public KMeansTest(Configuration testConfig) {
        super(testConfig);
        // TODO Auto-generated constructor stub
    }

    @Override
    protected JobGraph getJobGraph() throws Exception {

        
        //        String dataPointInput = "hdfs://localhost:9000/user/mhuelfen/input/vectors";
        String dataPointInput = "hdfs://localhost:9000/user/mhuelfen/input/small_one_file";
//      "hdfs://localhost:9000/user/mhuelfen/input/small__vectors":
                    
        String dataCount = "33222";
        String kValue = "2"; 
        String defaultParallelism = "2";
        
        String outputPath = "hdfs://localhost:9000/user/mhuelfen/output/kmeans";

        KmeansSeqFiles kmeans = new KmeansSeqFiles();
        Plan plan = kmeans.getPlan(defaultParallelism,dataPointInput,dataCount,kValue,outputPath);
        
        
        PactCompiler pc = new PactCompiler();
        OptimizedPlan op = pc.compile(plan);
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
