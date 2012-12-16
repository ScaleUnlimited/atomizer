package com.scaleunlimited.atomizer.flow;

import org.apache.log4j.Logger;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.atomizer.datum.AllAtomsDatum;
import com.scaleunlimited.atomizer.datum.DenaturedAttributeDatum;
import com.scaleunlimited.atomizer.datum.KnotDatum;
import com.scaleunlimited.cascading.LoggingFlowProcess;
import com.scaleunlimited.cascading.LoggingFlowReporter;
import com.scaleunlimited.cascading.NullContext;

@SuppressWarnings({"serial", "rawtypes"})
public class CreateKnottyTie extends BaseOperation<NullContext> implements Function<NullContext> {

    private static final Logger LOGGER = Logger.getLogger(CreateKnottyTie.class);

    private transient LoggingFlowProcess _flowProcess;

    public CreateKnottyTie() {
        super(KnotDatum.FIELDS);
        
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
        super.prepare(flowProcess, operationCall);
        LOGGER.info("Starting CreateKnottyTie");
        _flowProcess = new LoggingFlowProcess(flowProcess);
        _flowProcess.addReporter(new LoggingFlowReporter());
    }

    @Override
    public void cleanup(final FlowProcess flowProcess, final OperationCall<NullContext> operationCall) {
        super.cleanup(flowProcess, operationCall);
        LOGGER.info("Ending CreateKnottyTie");
        _flowProcess.dumpCounters();
    }


    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
        TupleEntryCollector outputCollector = functionCall.getOutputCollector();
        TupleEntry te = functionCall.getArguments();
        DenaturedAttributeDatum denaturedDatum = new DenaturedAttributeDatum(TupleEntry.select(DenaturedAttributeDatum.FIELDS, te));
        AllAtomsDatum allAtomsDatum = new AllAtomsDatum(TupleEntry.select(AllAtomsDatum.FIELDS, te));

        KnotDatum output = new KnotDatum(allAtomsDatum.getAtomId(), denaturedDatum.getDatasetId(), 
                        denaturedDatum.getRecordUuid(), denaturedDatum.getAttributeId());

        outputCollector.add(output.getTuple());
        flowProcess.increment(AtomizerCounters.TOTAL_KNOTS, 1);
    }
}
