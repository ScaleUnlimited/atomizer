package com.scaleunlimited.atomizer.flow;

import java.security.InvalidParameterException;

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.log4j.Logger;

import com.scaleunlimited.atomizer.datum.AllAtomsDatum;
import com.scaleunlimited.atomizer.datum.AtomizeDatum;
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

// AllAtoms is a sub assembly which assigns id numbers to atoms roughly based on frequency of 
// occurrence across all data sets, and keeps the count of each atom.
//
// Generates atomId, atom, count tuples.

@SuppressWarnings("serial")
public class AllAtoms extends SubAssembly {

    public static final String ALLATOMS_PIPE_NAME = "allatoms";

    @SuppressWarnings({"rawtypes"})
    private static class AssignAtomId extends BaseOperation<NullContext> implements Function<NullContext> {

        private static final Logger LOGGER = Logger.getLogger(AssignAtomId.class);

        private int _numTasks;
        private transient LoggingFlowProcess _flowProcess;
        private transient int _taskId;


        public AssignAtomId(int numTasks) {
            super(AllAtomsDatum.FIELDS);
            _numTasks = numTasks;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);
            LOGGER.info("Starting AssignAtomId");
            _flowProcess = new LoggingFlowProcess(flowProcess);
            _flowProcess.addReporter(new LoggingFlowReporter());
            String stringProperty = flowProcess.getStringProperty("mapred.task.id");
            
            // We want the taskId to start with 1 (alternatively we could count the tuples from 1...)
            if (stringProperty != null) {
                TaskAttemptID attemptId = TaskAttemptID.forName(stringProperty);
                _taskId = attemptId.getTaskID().getId() + 1;
            } else {
                // TODO VMa - should this throw an error instead ?
                _taskId = 1; 
            }
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
            AtomizeDatum datum = new AtomizeDatum(functionCall.getArguments().getTuple());
            int atomId = datum.getTupleNumber() * _numTasks + _taskId;
            AllAtomsDatum output = new AllAtomsDatum(atomId, datum.getAtom(), datum.getCount());
            outputCollector.add(output.getTuple());
            _flowProcess.increment(AtomizerCounters.ALL_ATOMS, 1);
        }
    }
    
    public AllAtoms(Pipe atomCountPipe, int numTasks) {
        
        Pipe allAtomsPipe = new Pipe(ALLATOMS_PIPE_NAME, atomCountPipe);
        allAtomsPipe = new Each(allAtomsPipe, new AssignAtomId(numTasks));
        setTails(allAtomsPipe);
    }
    
    
    public Pipe getAllAtomTailPipe() {
        return getTailPipe(ALLATOMS_PIPE_NAME);
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
