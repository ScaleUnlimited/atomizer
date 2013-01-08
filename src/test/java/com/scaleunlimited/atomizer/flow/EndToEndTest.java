package com.scaleunlimited.atomizer.flow;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;

import com.scaleunlimited.atomizer.extractor.BaseExtractor;
import com.scaleunlimited.atomizer.extractor.SimpleExtractor;
import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.FlowResult;
import com.scaleunlimited.cascading.FlowRunner;
import com.scaleunlimited.cascading.hadoop.HadoopPlatform;

public class EndToEndTest extends AbstractFlowTest {

    private static final String WORKING_DIR = "build/it/AllFlowsIntegrationTest";

    @Test
    public void testAllFlows() throws Exception {
        
        // Create a workflow to test all the SubAssemblies 
        FlowResult result = FlowRunner.run(createEndToEndFlow());

        // Assert that we got the desired number of knots
        assertEquals(10, result.getCounterValue(AtomizerCounters.TOTAL_KNOTS));
        
        // TODO VMa - verify expected values of knots

    }
    
    @SuppressWarnings("rawtypes")
    private static Flow createEndToEndFlow() throws Exception {
        
        BasePlatform platform = new HadoopPlatform(EndToEndTest.class);
        platform.setJobPollingInterval(CASCADING_LOCAL_JOB_POLLING_INTERVAL);

        
        BasePath recordsPath = platform.makePath("src/test/resources/records.txt");
        Tap recordsSource = platform.makeTap(platform.makeTextScheme(), recordsPath);
        Pipe recordsPipe = new Pipe("records");
        
        BasePath datasetAttributeRecordsPath = platform.makePath("src/test/resources/datasetId_to_attributeRecordId.txt");
        Tap datasetAttributeRecordsSource = platform.makeTap(platform.makeTextScheme(), datasetAttributeRecordsPath);
        Pipe datasetAttributeRecordsPipe = new Pipe("MetaDatasetRecord pipe");

        Map<String, Tap>sources = new HashMap<String, Tap>();
        sources.put(recordsPipe.getName(), recordsSource);
        sources.put(datasetAttributeRecordsPipe.getName(), datasetAttributeRecordsSource);

        recordsPipe = new Each(recordsPipe, new CreateRecordsDatumFromText());
        datasetAttributeRecordsPipe = new Each(datasetAttributeRecordsPipe, new CreateDatasetRecordsDatumFromText());

        Map<String, String> attributeNameToIdMap = readMapFile("src/test/resources/attributeId_to_attributeName.txt", true);
        Map<String, String> attributeIdToAnchorIdMap = readMapFile("src/test/resources/attributeId_to_anchorId.txt", false);
        List<String> attributeRecords = readFileLines("src/test/resources/attributeRecordId_to_attributeId.txt");
        BaseExtractor extractor = new SimpleExtractor(attributeNameToIdMap, attributeIdToAnchorIdMap, attributeRecords);
        Denature denature = new Denature(recordsPipe, datasetAttributeRecordsPipe, extractor);

        
        Pipe denaturedAttributesPipe = new Pipe("denatured attributes", denature.getDenaturedTailPipe());
        Atomize atomize = new Atomize(denaturedAttributesPipe);
        
        Pipe atomCounts = new Pipe("atom counts", atomize.getAtomizeTailPipe());
        AllAtoms allAtoms = new AllAtoms(atomCounts, 1);    // TODO VMa - figure out numTasks

        Knot knot = new Knot(denaturedAttributesPipe, allAtoms.getAllAtomTailPipe());

        BasePath workingDirPath = platform.makePath(WORKING_DIR);
        BasePath knotPath = platform.makePath(workingDirPath, "knot");
        Tap knotSink = platform.makeTap(platform.makeTextScheme(), knotPath, SinkMode.REPLACE);
        
        FlowConnector flowConnector = platform.makeFlowConnector();
        return flowConnector.connect(sources, knotSink, knot.getKnotTailPipe());

    }
}
