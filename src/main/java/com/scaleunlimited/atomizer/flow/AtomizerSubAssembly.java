package com.scaleunlimited.atomizer.flow;

import java.security.InvalidParameterException;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;

@SuppressWarnings("serial")
public abstract class AtomizerSubAssembly extends SubAssembly {

    public Pipe getTailPipe(String pipeName) {
        String[] pipeNames = getTailNames();
        for (int i = 0; i < pipeNames.length; i++) {
            if (pipeName.equals(pipeNames[i])) {
                return getTails()[i];
            }
        }
        throw new InvalidParameterException("Invalid pipe name: " + pipeName);
    }

}
