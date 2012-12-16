package com.scaleunlimited.atomizer.flow;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

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


public class KnotTest extends AbstractFlowTest {

    private static final String WORKING_DIR = "build/test/KnotTest";
    
    @Test
    public void testKnot() throws Exception {
        
        // Create a workflow to test the Knot SubAssembly
        FlowResult result = FlowRunner.run(createFlow());

        // Assert that we got the desired number of knots
        assertEquals(20, result.getCounterValue(AtomizerCounters.TOTAL_KNOTS));

    }
    
    @SuppressWarnings("rawtypes")
    private static Flow createFlow() throws Exception {
        BasePlatform platform = new LocalPlatform(KnotTest.class);
        platform.setJobPollingInterval(CASCADING_LOCAL_JOB_POLLING_INTERVAL);
        
        BasePath denaturedAttributesPath = platform.makePath("src/test/resources/denatured_attributes.txt");
        Tap denaturedAttributesSource = platform.makeTap(platform.makeTextScheme(), denaturedAttributesPath);
        Pipe denaturedAttributesPipe = new Pipe("denatured attributes");

        BasePath allAtomsPath = platform.makePath("src/test/resources/allatoms.txt");
        Tap allAtomsSource = platform.makeTap(platform.makeTextScheme(), allAtomsPath);
        Pipe allAtomsPipe = new Pipe("all atoms");

        Map<String, Tap>sources = new HashMap<String, Tap>();
        sources.put(denaturedAttributesPipe.getName(), denaturedAttributesSource);
        sources.put(allAtomsPipe.getName(), allAtomsSource);

        denaturedAttributesPipe = new Each(denaturedAttributesPipe, new CreateDenaturedAttributeDatumFromText());
        allAtomsPipe = new Each(allAtomsPipe, new CreateAllAtomsFromText());

        Knot knot = new Knot(denaturedAttributesPipe, allAtomsPipe);
        
        BasePath workingDirPath = platform.makePath(WORKING_DIR);
        BasePath knotPath = platform.makePath(workingDirPath, "knot");
        Tap knotSink = platform.makeTap(platform.makeTextScheme(), knotPath, SinkMode.REPLACE);
        
        FlowConnector flowConnector = platform.makeFlowConnector();
        return flowConnector.connect(sources, knotSink, knot.getKnotTailPipe());
       
    }
    
}
