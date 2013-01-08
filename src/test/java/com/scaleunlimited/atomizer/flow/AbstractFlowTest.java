package com.scaleunlimited.atomizer.flow;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;

import com.scaleunlimited.atomizer.datum.AllAtomsDatum;
import com.scaleunlimited.atomizer.datum.AtomizeDatum;
import com.scaleunlimited.atomizer.datum.DenaturedAttributeDatum;
import com.scaleunlimited.atomizer.datum.DatasetAttributeRecordDatum;
import com.scaleunlimited.atomizer.datum.RecordDatum;
import com.scaleunlimited.cascading.NullContext;

public abstract class AbstractFlowTest {

    private static final Logger LOGGER = Logger.getLogger(AbstractFlowTest.class);

    public static final long CASCADING_LOCAL_JOB_POLLING_INTERVAL = 100;

    @SuppressWarnings("serial")
    public static class CreateRecordsDatumFromText extends BaseOperation<NullContext> implements Function<NullContext> {

        public CreateRecordsDatumFromText() {
            super(RecordDatum.FIELDS);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {

            String line = functionCall.getArguments().getString("line");
            String[] split = line.split("\t");
            if (split.length == 3) {
                Map<String, String> attributeMap = new HashMap<String, String>();
                String[] keyValPairs = split[2].split(",");
                for (String keyValPair : keyValPairs) {
                    String[] entry = keyValPair.split("/");
                    attributeMap.put(entry[0], entry[1]);
                }
                RecordDatum datum = new RecordDatum(split[0], split[1], attributeMap);
                functionCall.getOutputCollector().add(datum.getTuple());
            } else {
                LOGGER.error("Invalid import line: " + line);
            }
        }
    }

    @SuppressWarnings("serial")
    public static class CreateDatasetRecordsDatumFromText extends BaseOperation<NullContext> implements Function<NullContext> {

        public CreateDatasetRecordsDatumFromText() {
            super(DatasetAttributeRecordDatum.FIELDS);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {

            String line = functionCall.getArguments().getString("line");
            String[] split = line.split("\t");
            if (split.length == 2) {
                DatasetAttributeRecordDatum datum = new DatasetAttributeRecordDatum(split[0], split[1]);
                functionCall.getOutputCollector().add(datum.getTuple());
            } else {
                LOGGER.error("Invalid import line: " + line);
            }
        }
    }

    @SuppressWarnings({ "serial", "rawtypes" })
    public static class CreateDenaturedAttributeDatumFromText extends BaseOperation<NullContext> implements Function<NullContext> {

        public CreateDenaturedAttributeDatumFromText() {
            super(DenaturedAttributeDatum.FIELDS);
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {

            String line = functionCall.getArguments().getString("line");
            String[] split = line.split("\t");
            if (split.length == 5) {

                // datasetId, recordUuid, anchorId, attributeId, atom
                DenaturedAttributeDatum datum = new DenaturedAttributeDatum(split[0], split[1], split[2], split[3], split[4]);
                functionCall.getOutputCollector().add(datum.getTuple());
            } else {
                LOGGER.error("Invalid import line: " + line);
            }
        }
    }

    @SuppressWarnings({ "serial", "rawtypes" })
    public static class CreateAtomCountFromText extends BaseOperation<NullContext> implements Function<NullContext> {

        public CreateAtomCountFromText() {
            super(AtomizeDatum.FIELDS);
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {

            String line = functionCall.getArguments().getString("line");
            String[] split = line.split("\t");
            if (split.length == 3) {
                // atom, count, taskId
                AtomizeDatum datum = new AtomizeDatum(split[0], Integer.parseInt(split[1]), Integer.parseInt(split[2]));
                functionCall.getOutputCollector().add(datum.getTuple());
            } else {
                LOGGER.error("Invalid import line: " + line);
            }
        }
    }

    @SuppressWarnings({ "serial", "rawtypes" })
    public static class CreateAllAtomsFromText extends BaseOperation<NullContext> implements Function<NullContext> {

        public CreateAllAtomsFromText() {
            super(AllAtomsDatum.FIELDS);
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {

            String line = functionCall.getArguments().getString("line");
            String[] split = line.split("\t");
            if (split.length == 3) {
                // atomId, atom, count
                AllAtomsDatum datum = new AllAtomsDatum(Integer.parseInt(split[0]), split[1], Integer.parseInt(split[2]));
                functionCall.getOutputCollector().add(datum.getTuple());
            } else {
                LOGGER.error("Invalid import line: " + line);
            }
        }
    }

    public static Map<String, String> readMapFile(String fileName, boolean flip) throws IOException {
        Map<String, String> map = new HashMap<String, String>();

        InputStream is = new FileInputStream(new File(fileName));
        try {
            List<String> lines = IOUtils.readLines(is);
            for (String line : lines) {
                if (line.length() > 0 && !line.startsWith("#")) {
                    String[] split = line.split("\t");
                    if (split.length == 2) {
                        if (flip) {
                            map.put(split[1], split[0]);

                        } else {
                            map.put(split[0], split[1]);
                        }
                    } else {
                        System.out.println("Invalid line - expected 2 tab separated fields; instead got :" + line);
                    }
                }
            }
        } finally {
            IOUtils.closeQuietly(is);
        }

        return map;
    }

    public static List<String> readFileLines(String fileName) throws IOException {
        List<String> lines = null;

        InputStream is = new FileInputStream(new File(fileName));
        try {
            lines = IOUtils.readLines(is);
        } finally {
            IOUtils.closeQuietly(is);
        }

        return lines;
    }

}
