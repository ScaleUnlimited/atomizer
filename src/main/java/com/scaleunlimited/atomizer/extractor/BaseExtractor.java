package com.scaleunlimited.atomizer.extractor;

import java.io.Serializable;
import java.util.List;

import com.scaleunlimited.atomizer.datum.DenaturedAttributeDatum;

@SuppressWarnings("serial")
public abstract class BaseExtractor implements Serializable {

    public abstract List<DenaturedAttributeDatum> extract(String datasetId, String recordUuid, String metaId, String attributeName, String attributeValue);
}
