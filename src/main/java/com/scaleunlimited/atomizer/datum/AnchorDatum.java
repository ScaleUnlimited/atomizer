package com.scaleunlimited.atomizer.datum;

import cascading.tuple.Fields;

import com.scaleunlimited.cascading.BaseDatum;

// datasetId (String), recordUuid (String), anchorId (String), attribute name (String), atom (String), confidence level (float)

public class AnchorDatum extends BaseDatum {

    public static final String ATOM_FN = BaseDatum.fieldName(AnchorDatum.class, "atom");
    
    public static final Fields FIELDS = new Fields(ATOM_FN);
}
