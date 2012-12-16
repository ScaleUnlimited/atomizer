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
import com.scaleunlimited.cascading.local.LocalPlatform;


public class AtomizeTest extends AbstractFlowTest {

    private static final String WORKING_DIR = "build/test/AtomizeTest";
    
    @Test
    public void testAtomize() throws Exception {
        
        // Create a workflow to test the Atomizer SubAssembly
        FlowResult result = FlowRunner.run(createFlow());
        
        // Assert that we got the desired number of anchors
        assertEquals(16, result.getCounterValue(AtomizerCounters.TOTAL_ATOMIZE));

    }
    
    
    @SuppressWarnings("rawtypes")
    private static Flow createFlow() throws Exception {
        BasePlatform platform = new LocalPlatform(AtomizeTest.class);
        platform.setJobPollingInterval(CASCADING_LOCAL_JOB_POLLING_INTERVAL);
        
        BasePath recordsPath = platform.makePath("src/test/resources/denatured_attributes.txt");
        Tap recordsSource = platform.makeTap(platform.makeTextScheme(), recordsPath);

        Pipe denaturedAttributesPipe = new Pipe("denatured attributes");
        denaturedAttributesPipe = new Each(denaturedAttributesPipe, new CreateDenaturedAttributeDatumFromText());
        Atomize atomize = new Atomize(denaturedAttributesPipe);
        
        BasePath workingDirPath = platform.makePath(WORKING_DIR);
        BasePath atomCountsPath = platform.makePath(workingDirPath, "atomize");
        Tap atomizeSink = platform.makeTap(platform.makeTextScheme(), atomCountsPath, SinkMode.REPLACE);
        
        FlowConnector flowConnector = platform.makeFlowConnector();
        return flowConnector.connect(recordsSource, atomizeSink, atomize.getAtomizeTailPipe());
       
    }
}
