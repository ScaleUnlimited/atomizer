package com.scaleunlimited.atomizer.flow;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.atomizer.datum.DenaturedAttributeDatum;
import com.scaleunlimited.atomizer.datum.RecordDatum;
import com.scaleunlimited.atomizer.extractor.BaseExtractor;
import com.scaleunlimited.cascading.LoggingFlowProcess;
import com.scaleunlimited.cascading.LoggingFlowReporter;
import com.scaleunlimited.cascading.NullContext;

//Takes in 
//  - tuples that contain datasetId, recordUuid, and a key/value (attribute name/value) map.
//  - a BaseExtractor for extracting anchorId and attributeId from meta records
//
// Outputs DenaturedAttributeDatums

@SuppressWarnings("serial")
public class Denature extends AtomizerSubAssembly {

    public static final String DENATURED_PIPE_NAME = "denatured";

    @SuppressWarnings({"rawtypes"})
    private static class ExtractAnchorsFromRecord extends BaseOperation<NullContext> implements Function<NullContext> {
        private static final Logger LOGGER = Logger.getLogger(ExtractAnchorsFromRecord.class);

        private BaseExtractor _extractor;
        private transient LoggingFlowProcess _flowProcess;

        public ExtractAnchorsFromRecord(BaseExtractor extractor) {
            super(DenaturedAttributeDatum.FIELDS);
            
            _extractor = extractor;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);
            LOGGER.info("Starting ExtractAnchorsFromRecord");
            _flowProcess = new LoggingFlowProcess(flowProcess);
            _flowProcess.addReporter(new LoggingFlowReporter());
        }

        @Override
        public void cleanup(final FlowProcess flowProcess, final OperationCall<NullContext> operationCall) {
            super.cleanup(flowProcess, operationCall);
            LOGGER.info("Ending ExtractAnchorsFromRecord");
            _flowProcess.dumpCounters();
        }


        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
            TupleEntryCollector outputCollector = functionCall.getOutputCollector();
            RecordDatum recordDatum = new RecordDatum(functionCall.getArguments().getTuple());
            String datasetId = recordDatum.getDatasetId();
            String recordUuid = recordDatum.getRecordUuid();
            Map<String, String> attributeMap = recordDatum.getAttributeMap();
            for (Entry<String, String> entry : attributeMap.entrySet()) {
                List<DenaturedAttributeDatum> anchorDatums = _extractor.parse(datasetId, recordUuid, entry.getKey(), entry.getValue());
                for (DenaturedAttributeDatum anchorDatum : anchorDatums) {
                    if (anchorDatum.getAnchorId() != null) {
                        outputCollector.add(anchorDatum.getTuple());
                        _flowProcess.increment(AtomizerCounters.TOTAL_ANCHORS, 1);
                    } else {
                        _flowProcess.increment(AtomizerCounters.NO_ANCHORS, 1);
                    }
                }
            }
        }
    }
    public Denature(Pipe recordPipe, BaseExtractor extractor) {
        
        Pipe denaturedPipe = new Pipe(DENATURED_PIPE_NAME, recordPipe);
        denaturedPipe = new Each(denaturedPipe, new ExtractAnchorsFromRecord(extractor));
        
        setTails(denaturedPipe);
    }
    
    public Pipe getDenaturedTailPipe() {
        return getTailPipe(DENATURED_PIPE_NAME);
    }
}
