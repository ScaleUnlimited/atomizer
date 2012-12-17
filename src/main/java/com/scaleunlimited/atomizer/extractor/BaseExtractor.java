package com.scaleunlimited.atomizer.extractor;

import java.util.List;

import com.scaleunlimited.atomizer.datum.DenaturedAttributeDatum;

public abstract class BaseExtractor {

    public abstract List<DenaturedAttributeDatum> parse(String datasetId, String recordUuid, String attributeName, String attributeValue);
}
