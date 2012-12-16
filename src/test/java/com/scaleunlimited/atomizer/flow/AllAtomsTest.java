package com.scaleunlimited.atomizer.flow;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.FlowResult;
import com.scaleunlimited.cascading.FlowRunner;
import com.scaleunlimited.cascading.hadoop.HadoopPlatform;


public class AllAtomsTest extends AbstractFlowTest {

    private static final String WORKING_DIR = "build/test/AllAtoms";
    
    @Test
    public void testAllAtoms() throws Exception {
        
        // Create a workflow to test the Atomizer SubAssembly
        FlowResult result = FlowRunner.run(createFlow());
        
        // Assert that we got the desired number of anchors
        assertEquals(43, result.getCounterValue(AtomizerCounters.ALL_ATOMS));

    }
    
    
    @SuppressWarnings("rawtypes")
    private static Flow createFlow() throws Exception {
        BasePlatform platform = new HadoopPlatform(AllAtomsTest.class);
        platform.setJobPollingInterval(CASCADING_LOCAL_JOB_POLLING_INTERVAL);
        
        BasePath recordsPath = platform.makePath("src/test/resources/atomize.txt");
        Tap recordsSource = platform.makeTap(platform.makeTextScheme(), recordsPath);

        Pipe atomCounts = new Pipe("atom counts");
        atomCounts = new Each(atomCounts, new CreateAtomCountFromText());
        AllAtoms allAtoms = new AllAtoms(atomCounts, 1);    // TODO VMa - figure out numTasks
        
        BasePath workingDirPath = platform.makePath(WORKING_DIR);
        BasePath allAtomsPath = platform.makePath(workingDirPath, "allAtoms");
        Tap allAtomsSink = platform.makeTap(platform.makeTextScheme(), allAtomsPath, SinkMode.REPLACE);
        
        FlowConnector flowConnector = platform.makeFlowConnector();
        return flowConnector.connect(recordsSource, allAtomsSink, allAtoms.getAllAtomTailPipe());
       
    }
}
