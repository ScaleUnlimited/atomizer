package com.scaleunlimited.atomizer.datum;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.scaleunlimited.cascading.BaseDatum;

// record has datasetId, recordUuid, and a key/value (attribute name/value) map

@SuppressWarnings("serial")
public class KnotDatum extends BaseDatum {

    public static final String ATOM_ID_FN = BaseDatum.fieldName(KnotDatum.class, "atomId");
    public static final String DATASET_ID_FN = BaseDatum.fieldName(KnotDatum.class, "datasetId");
    public static final String RECORD_UUID_FN = BaseDatum.fieldName(KnotDatum.class, "recordUuid");
    public static final String ATTRIBUTE_ID_FN = BaseDatum.fieldName(KnotDatum.class, "attributeId");

    public static final Fields FIELDS = new Fields(
                    ATOM_ID_FN,
                    DATASET_ID_FN,
                    RECORD_UUID_FN,
                    ATTRIBUTE_ID_FN);
    
    public KnotDatum(int atomId, String datasetId, String recordUuid, String attributeId) {
        super(FIELDS);
        setAtomId(atomId);
        setDatasetId(datasetId);
        setRecordUuid(recordUuid);
        setAttributeId(attributeId);
    }
    
    public KnotDatum(Tuple t) {
        super(FIELDS, t);
    }

    public int getAtomId() {
        return _tupleEntry.getInteger(ATOM_ID_FN);
    }
    
    public void setAtomId(int atomId) {
        _tupleEntry.set(ATOM_ID_FN, atomId);
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
    
    public String getAttributeId() {
        return _tupleEntry.getString(ATTRIBUTE_ID_FN);
    }
    
    public void setAttributeId(String attributeId) {
        _tupleEntry.set(ATTRIBUTE_ID_FN, attributeId);
    }

}
