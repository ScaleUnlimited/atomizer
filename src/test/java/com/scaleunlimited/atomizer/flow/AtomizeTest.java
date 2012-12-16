package com.scaleunlimited.atomizer.flow;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;

import com.scaleunlimited.atomizer.datum.AtomCountDatum;
import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.FlowResult;
import com.scaleunlimited.cascading.FlowRunner;
import com.scaleunlimited.cascading.LoggingFlowProcess;
import com.scaleunlimited.cascading.LoggingFlowReporter;
import com.scaleunlimited.cascading.NullContext;
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
    
    
    @SuppressWarnings({ "unchecked", "rawtypes", "serial" })
    private static class TestCounter extends BaseOperation<NullContext> implements Function<NullContext> {
        
        private transient LoggingFlowProcess _flowProcess;
        
        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);
            _flowProcess = new LoggingFlowProcess(flowProcess);
            _flowProcess.addReporter(new LoggingFlowReporter());
        }

        @Override
        public void cleanup(final FlowProcess flowProcess, final OperationCall<NullContext> operationCall) {
            super.cleanup(flowProcess, operationCall);
            _flowProcess.dumpCounters();
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
            AtomCountDatum datum = new AtomCountDatum(functionCall.getArguments().getTuple());
            _flowProcess.increment(AtomizerCounters.TOTAL_ATOMIZE, 1);
            functionCall.getOutputCollector().add(datum.getTuple());
            
        }
    }
    @SuppressWarnings("rawtypes")
    private static Flow createFlow() throws Exception {
        BasePlatform platform = new LocalPlatform(AtomizeTest.class);
        platform.setJobPollingInterval(CASCADING_LOCAL_JOB_POLLING_INTERVAL);
        
        BasePath recordsPath = platform.makePath("src/test/resources/denatured_attributes.txt");
        Tap recordsSource = platform.makeTap(platform.makeTextScheme(), recordsPath);

        Pipe denaturedAttributesPipe = new Pipe("denatured attributes");
        denaturedAttributesPipe = new Each(denaturedAttributesPipe, new CreateDenaturedAttributeDatumFromText());
        Atomize atomizer = new Atomize(denaturedAttributesPipe);
        // We don't have any counters set up in the sub assembly so for testing purpose lets create some.
        Pipe testCountPipe = new Each(atomizer.getAtomCountsTailPipe(), new TestCounter());
        
        BasePath workingDirPath = platform.makePath(WORKING_DIR);
        BasePath atomCountsPath = platform.makePath(workingDirPath, "atom-counts");
        Tap atomsCountSink = platform.makeTap(platform.makeTextScheme(), atomCountsPath, SinkMode.REPLACE);
        
        FlowConnector flowConnector = platform.makeFlowConnector();
        return flowConnector.connect(recordsSource, atomsCountSink, testCountPipe);
       
    }
}
