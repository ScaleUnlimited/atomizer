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
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.atomizer.datum.DenaturedAttributeDatum;
import com.scaleunlimited.atomizer.datum.MetaDatasetRecordDatum;
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


        // The input is a hash joined steam of RecordDatums and MetaDatasetRecordDatums
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
            TupleEntryCollector outputCollector = functionCall.getOutputCollector();
            TupleEntry te = functionCall.getArguments();
            RecordDatum recordDatum = new RecordDatum(TupleEntry.select(RecordDatum.FIELDS, te));
            String datasetId = recordDatum.getDatasetId();
            String recordUuid = recordDatum.getRecordUuid();
            MetaDatasetRecordDatum metaDatasetDatum = new MetaDatasetRecordDatum(TupleEntry.select(MetaDatasetRecordDatum.FIELDS, te));

            Map<String, String> attributeMap = recordDatum.getAttributeMap();
            for (Entry<String, String> entry : attributeMap.entrySet()) {
                List<DenaturedAttributeDatum> anchorDatums = _extractor.extract(datasetId, recordUuid, metaDatasetDatum.getMetaId(), entry.getKey(), entry.getValue());
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
    public Denature(Pipe recordPipe, Pipe metaDatasetRecordPipe, BaseExtractor extractor) {
        
        Pipe hashPipe = new HashJoin(recordPipe, new Fields(RecordDatum.DATASET_ID_FN), 
                        metaDatasetRecordPipe, new Fields(MetaDatasetRecordDatum.DATASET_ID_FN));
        
        Pipe denaturedPipe = new Pipe(DENATURED_PIPE_NAME, hashPipe);
        denaturedPipe = new Each(denaturedPipe, new ExtractAnchorsFromRecord(extractor));
        
        setTails(denaturedPipe);
    }
    
    public Pipe getDenaturedTailPipe() {
        return getTailPipe(DENATURED_PIPE_NAME);
    }
}
