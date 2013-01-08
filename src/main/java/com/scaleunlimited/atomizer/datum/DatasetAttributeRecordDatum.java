package com.scaleunlimited.atomizer.datum;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.scaleunlimited.cascading.BaseDatum;


@SuppressWarnings("serial")
public class DatasetAttributeRecordDatum extends BaseDatum {

    public static final String DATASET_ID_FN = BaseDatum.fieldName(DatasetAttributeRecordDatum.class, "datasetId");
    public static final String ATTRIBUTE_RECORD_ID_FN = BaseDatum.fieldName(DatasetAttributeRecordDatum.class, "attributeRecordId");

    public static final Fields FIELDS = new Fields(
                    DATASET_ID_FN,
                    ATTRIBUTE_RECORD_ID_FN);
    
    public DatasetAttributeRecordDatum(String datasetId, String attributeRecordId) {
        super(FIELDS);
        setDatasetId(datasetId);
        setAttributeRecordId(attributeRecordId);
    }
    
    public DatasetAttributeRecordDatum(Tuple t) {
        super(FIELDS, t);
    }

    public String getDatasetId() {
        return _tupleEntry.getString(DATASET_ID_FN);
    }
    
    public void setDatasetId(String datasetId) {
        _tupleEntry.set(DATASET_ID_FN, datasetId);
    }

    public String getAttributeRecordId() {
        return _tupleEntry.getString(ATTRIBUTE_RECORD_ID_FN);
    }
    
    public void setAttributeRecordId(String recordUuid) {
        _tupleEntry.set(ATTRIBUTE_RECORD_ID_FN, recordUuid);
    }
}
