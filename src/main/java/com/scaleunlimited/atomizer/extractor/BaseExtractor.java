package com.scaleunlimited.atomizer.extractor;

import java.util.List;

import com.scaleunlimited.atomizer.datum.DenaturedAttributeDatum;

public abstract class BaseExtractor {

    public abstract List<DenaturedAttributeDatum> extract(String datasetId, String recordUuid, String metaId, String attributeName, String attributeValue);
}
