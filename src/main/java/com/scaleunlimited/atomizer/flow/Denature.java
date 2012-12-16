package com.scaleunlimited.atomizer.flow;

import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.scaleunlimited.atomizer.datum.DenaturedAttributeDatum;
import com.scaleunlimited.atomizer.datum.RecordDatum;
import com.scaleunlimited.atomizer.parser.BaseParser;
import com.scaleunlimited.cascading.LoggingFlowProcess;
import com.scaleunlimited.cascading.LoggingFlowReporter;
import com.scaleunlimited.cascading.NullContext;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.TupleEntryCollector;

//Takes in 
//  - tuples that contain datasetId, recordUuid, and a key/value (attribute name/value) map.
//  - tuples that have datasetId, meta record id

@SuppressWarnings("serial")
public class Denature extends SubAssembly {

    public static final String ANCHORS_PIPE_NAME = "anchors";

    @SuppressWarnings({"rawtypes"})
    private static class ExtractAnchorsFromRecord extends BaseOperation<NullContext> implements Function<NullContext> {
        private static final Logger LOGGER = Logger.getLogger(ExtractAnchorsFromRecord.class);

        private BaseParser _parser;
        private transient LoggingFlowProcess _flowProcess;

        public ExtractAnchorsFromRecord(BaseParser parser) {
            super(DenaturedAttributeDatum.FIELDS);
            
            _parser = parser;
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
                List<DenaturedAttributeDatum> anchorDatums = _parser.parse(datasetId, recordUuid, entry.getKey(), entry.getValue());
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
    public Denature(Pipe recordPipe, BaseParser parser) {
        
        Pipe anchorPipe = new Pipe(ANCHORS_PIPE_NAME, recordPipe);
        anchorPipe = new Each(anchorPipe, new ExtractAnchorsFromRecord(parser));
        
        setTails(anchorPipe);
    }
    
    public Pipe getAnchorsTailPipe() {
        return getTailPipe(ANCHORS_PIPE_NAME);
    }
    
    private Pipe getTailPipe(String pipeName) {
        String[] pipeNames = getTailNames();
        for (int i = 0; i < pipeNames.length; i++) {
            if (pipeName.equals(pipeNames[i])) {
                return getTails()[i];
            }
        }
        throw new InvalidParameterException("Invalid pipe name: " + pipeName);
    }
}
