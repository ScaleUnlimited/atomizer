package com.scaleunlimited.atomizer.datum;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.scaleunlimited.cascading.BaseDatum;


@SuppressWarnings("serial")
public class MetaDatasetRecordDatum extends BaseDatum {

    public static final String DATASET_ID_FN = BaseDatum.fieldName(MetaDatasetRecordDatum.class, "datasetId");
    public static final String META_ID_FN = BaseDatum.fieldName(MetaDatasetRecordDatum.class, "metaId");

    public static final Fields FIELDS = new Fields(
                    DATASET_ID_FN,
                    META_ID_FN);
    
    public MetaDatasetRecordDatum(String datasetId, String metaId) {
        super(FIELDS);
        setDatasetId(datasetId);
        setMetaId(metaId);
    }
    
    public MetaDatasetRecordDatum(Tuple t) {
        super(FIELDS, t);
    }

    public String getDatasetId() {
        return _tupleEntry.getString(DATASET_ID_FN);
    }
    
    public void setDatasetId(String datasetId) {
        _tupleEntry.set(DATASET_ID_FN, datasetId);
    }

    public String getMetaId() {
        return _tupleEntry.getString(META_ID_FN);
    }
    
    public void setMetaId(String recordUuid) {
        _tupleEntry.set(META_ID_FN, recordUuid);
    }
}
