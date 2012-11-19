package com.scaleunlimited.atomizer.datum;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.scaleunlimited.cascading.BaseDatum;

@SuppressWarnings("serial")
public class AnchorDatum extends BaseDatum {

    public static final String DATASET_ID_FN = BaseDatum.fieldName(AnchorDatum.class, "datasetId");
    public static final String RECORD_UUID_FN = BaseDatum.fieldName(AnchorDatum.class, "recordUuid");
    public static final String ANCHOR_ID_FN = BaseDatum.fieldName(AnchorDatum.class, "anchorId");
    public static final String ATTRIBUTE_NAME_FN = BaseDatum.fieldName(AnchorDatum.class, "attributeName");
    public static final String ATOM_FN = BaseDatum.fieldName(AnchorDatum.class, "atom");
    public static final String CONFIDENCE_LEVEL_FN = BaseDatum.fieldName(AnchorDatum.class, "confidenceLevel");

    public static final Fields FIELDS = new Fields(
                    DATASET_ID_FN,
                    RECORD_UUID_FN,
                    ANCHOR_ID_FN,
                    ATTRIBUTE_NAME_FN,
                    ATOM_FN,
                    CONFIDENCE_LEVEL_FN);
    
    public AnchorDatum(String datasetId, String recordUuid, String anchorId, String attributeName, 
                    String atom, float confidenceLevel) {
        super(FIELDS);
        setDatasetId(datasetId);
        setRecordUuid(recordUuid);
        setAnchorId(anchorId);
        setAttributeName(attributeName);
        setAtom(atom);
        setConfidenceLevel(confidenceLevel);
    }
    
    public AnchorDatum(Tuple t) {
        super(FIELDS, t);
    }

    public String getDatasetId() {
        return _tupleEntry.getString(DATASET_ID_FN);
    }
    
    public void setDatasetId(String datasetId) {
        _tupleEntry.set(DATASET_ID_FN, datasetId);
    }

    public String getRecordUuid() {
        return _tupleEntry.getString(RECORD_UUID_FN);
    }
    
    public void setRecordUuid(String recordUuid) {
        _tupleEntry.set(RECORD_UUID_FN, recordUuid);
    }
    
    public String getAnchorId() {
        return _tupleEntry.getString(ANCHOR_ID_FN);
    }
    
    public void setAnchorId(String anchorId) {
        _tupleEntry.set(ANCHOR_ID_FN, anchorId);
    }
    
    public String getAttributeName() {
        return _tupleEntry.getString(ATTRIBUTE_NAME_FN);
    }
    
    public void setAttributeName(String attributeName) {
        _tupleEntry.set(ATTRIBUTE_NAME_FN, attributeName);
    }
    
    public String getAtom() {
        return _tupleEntry.getString(ATOM_FN);
    }
    
    public void setAtom(String atom) {
        _tupleEntry.set(ATOM_FN, atom);
    }

    public float getConfidenceLevel() {
        return _tupleEntry.getFloat(CONFIDENCE_LEVEL_FN);
    }
    
    public void setConfidenceLevel(float confidenceLevel) {
        _tupleEntry.set(CONFIDENCE_LEVEL_FN, confidenceLevel);
    }

}
