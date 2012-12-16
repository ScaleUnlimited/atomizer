package com.scaleunlimited.atomizer.flow;

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.log4j.Logger;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.atomizer.datum.AllAtomsDatum;
import com.scaleunlimited.atomizer.datum.AtomCountDatum;
import com.scaleunlimited.cascading.LoggingFlowProcess;
import com.scaleunlimited.cascading.LoggingFlowReporter;
import com.scaleunlimited.cascading.NullContext;

@SuppressWarnings({"serial", "rawtypes"})
public class AssignAtomId extends BaseOperation<NullContext> implements Function<NullContext> {

    private static final Logger LOGGER = Logger.getLogger(AssignAtomId.class);

    private transient LoggingFlowProcess _flowProcess;

    private transient int _attemptId;
    private transient int _tupleNumber;

    public AssignAtomId() {
        super(AllAtomsDatum.FIELDS);
        
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
        super.prepare(flowProcess, operationCall);
        LOGGER.info("Starting AssignAtomId");
        _flowProcess = new LoggingFlowProcess(flowProcess);
        _flowProcess.addReporter(new LoggingFlowReporter());
        String stringProperty = flowProcess.getStringProperty("mapred.task.id");
        if (stringProperty != null) {
            TaskAttemptID attempt = TaskAttemptID.forName(stringProperty);
            _attemptId = attempt.getId() + 1;  // we don't want a zero value
        } else {
            // TODO VMa - should this throw an error instead ?
            _attemptId = 1; 
        }
        _tupleNumber = 1;
    }

    @Override
    public void cleanup(final FlowProcess flowProcess, final OperationCall<NullContext> operationCall) {
        super.cleanup(flowProcess, operationCall);
        LOGGER.info("Ending AssignAtomId");
        _flowProcess.dumpCounters();
    }


    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
        TupleEntryCollector outputCollector = functionCall.getOutputCollector();
        AtomCountDatum datum = new AtomCountDatum(functionCall.getArguments().getTuple());
        AllAtomsDatum output = new AllAtomsDatum(_tupleNumber * _attemptId, datum.getAtom(), datum.getCount());
        outputCollector.add(output.getTuple());
        _flowProcess.increment(AtomizerCounters.ALL_ATOMS, 1);
        _tupleNumber++;
    }
}
