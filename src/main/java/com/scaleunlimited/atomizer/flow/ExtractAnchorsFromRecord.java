package com.scaleunlimited.atomizer.flow;

import com.scaleunlimited.atomizer.datum.AnchorDatum;
import com.scaleunlimited.atomizer.parser.BaseParser;
import com.scaleunlimited.cascading.NullContext;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;

@SuppressWarnings("serial")
public class ExtractAnchorsFromRecord extends BaseOperation<NullContext> implements Function<NullContext> {

    private BaseParser _parser;
    
    public ExtractAnchorsFromRecord(BaseParser parser) {
        super(AnchorDatum.FIELDS);
        
        _parser = parser;
    }
    
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
        // TODO set up RecordDatum, call parser, emit results.
        // TODO Auto-generated method stub
        
    }

}
