package com.scaleunlimited.atomizer.flow;

import java.security.InvalidParameterException;

import com.scaleunlimited.atomizer.parser.BaseParser;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;

//Takes in 
//  - tuples that contain datasetId, recordUuid, and a key/value (attribute name/value) map.
//  - tuples that have datasetId, meta record id

@SuppressWarnings("serial")
public class Denature extends SubAssembly {

    public static final String ANCHORS_PIPE_NAME = "anchors";

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
