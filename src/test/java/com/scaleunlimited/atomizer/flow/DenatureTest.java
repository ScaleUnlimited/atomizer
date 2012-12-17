package com.scaleunlimited.atomizer.flow;

import static org.junit.Assert.assertEquals;

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
import com.scaleunlimited.cascading.local.LocalPlatform;


public class DenatureTest extends AbstractFlowTest {

    private static final String WORKING_DIR = "build/test/DenatureTest";
    
    @Test
    public void testDenature() throws Exception {
        
        // Create a workflow to test the Denature SubAssembly
        FlowResult result = FlowRunner.run(createFlow());

        // Assert that we got the desired number of anchors
        assertEquals(10, result.getCounterValue(AtomizerCounters.TOTAL_ANCHORS));

    }
    
    @SuppressWarnings("rawtypes")
    private static Flow createFlow() throws Exception {
        BasePlatform platform = new LocalPlatform(DenatureTest.class);
        platform.setJobPollingInterval(CASCADING_LOCAL_JOB_POLLING_INTERVAL);
        
        BasePath recordsPath = platform.makePath("src/test/resources/records.txt");
        Tap recordsSource = platform.makeTap(platform.makeTextScheme(), recordsPath);

        Pipe recordsPipe = new Pipe("records");
        recordsPipe = new Each(recordsPipe, new CreateRecordsDatumFromText());
        Map<String, String> attributeNameToIdMap = readMapFile("src/test/resources/meta_anchor_attribute.txt", true);
        Map<String, String> attributeIdToAnchorIdMap = readMapFile("src/test/resources/meta_attribute.txt", false);
        Map<String, String> datasetIdToMetaRecord = readMapFile("src/test/resources/meta_dataset_record.txt", false);
        List<String> metaRecords = readFileLines("src/test/resources/meta_record.txt");
        BaseExtractor extractor = new SimpleExtractor(attributeNameToIdMap, attributeIdToAnchorIdMap, 
                        datasetIdToMetaRecord, metaRecords);
        Denature denature = new Denature(recordsPipe, extractor);
        
        BasePath workingDirPath = platform.makePath(WORKING_DIR);
        BasePath denaturedPath = platform.makePath(workingDirPath, "denatured");
        Tap denaturedSink = platform.makeTap(platform.makeTextScheme(), denaturedPath, SinkMode.REPLACE);
        
        FlowConnector flowConnector = platform.makeFlowConnector();
        return flowConnector.connect(recordsSource, denaturedSink, denature.getDenaturedTailPipe());
       
    }
}
