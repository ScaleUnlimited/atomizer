package com.scaleunlimited.atomizer.datum;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.scaleunlimited.cascading.BaseDatum;

@SuppressWarnings("serial")
public class DenaturedAttributeDatum extends BaseDatum {

    public static final String DATASET_ID_FN = BaseDatum.fieldName(DenaturedAttributeDatum.class, "datasetId");
    public static final String RECORD_UUID_FN = BaseDatum.fieldName(DenaturedAttributeDatum.class, "recordUuid");
    public static final String ANCHOR_ID_FN = BaseDatum.fieldName(DenaturedAttributeDatum.class, "anchorId");
    public static final String ATTRIBUTE_ID_FN = BaseDatum.fieldName(DenaturedAttributeDatum.class, "attributeID");
    public static final String ATOM_FN = BaseDatum.fieldName(DenaturedAttributeDatum.class, "atom");

    public static final Fields FIELDS = new Fields(
                    DATASET_ID_FN,
                    RECORD_UUID_FN,
                    ANCHOR_ID_FN,
                    ATTRIBUTE_ID_FN,
                    ATOM_FN);
    
    public DenaturedAttributeDatum(String datasetId, String recordUuid, String anchorId, String attributeId, 
                    String atom) {
        super(FIELDS);
        setDatasetId(datasetId);
        setRecordUuid(recordUuid);
        setAnchorId(anchorId);
        setAttributeId(attributeId);
        setAtom(atom);
    }
    
    public DenaturedAttributeDatum(Tuple t) {
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
    
    public String getAttributeId() {
        return _tupleEntry.getString(ATTRIBUTE_ID_FN);
    }
    
    public void setAttributeId(String attributeId) {
        _tupleEntry.set(ATTRIBUTE_ID_FN, attributeId);
    }
    
    public String getAtom() {
        return _tupleEntry.getString(ATOM_FN);
    }
    
    public void setAtom(String atom) {
        _tupleEntry.set(ATOM_FN, atom);
    }

}
