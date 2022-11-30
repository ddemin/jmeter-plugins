package org.apache.jmeter.visualizers.backend.influxdb2.container;

import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.jmeter.visualizers.backend.influxdb2.util.Utils.parseStringToMap;

public class OperationMetaBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(OperationMetaBuffer.class);

    private final Map<String, Map<MetaTypeEnum, List<Map.Entry<String, String>>>> buffer
            = Collections.synchronizedMap(new HashMap<>());

    public Map<String, Map<MetaTypeEnum, List<Map.Entry<String, String>>>> getBuffer() {
        return buffer;
    }

    public void putLabelsMeta(String sampleName, String labels) {
        getResultBucket(sampleName)
                .computeIfAbsent(MetaTypeEnum.LABEL, k -> Collections.synchronizedList(new ArrayList<>()))
                .addAll(
                        parseStringToMap(labels).entrySet().stream().toList()
                );
    }

    public void putLabelsMeta(SampleResult sampleResult, String labels) {
        putLabelsMeta(sampleResult.getSampleLabel(), labels);
    }

    Map<MetaTypeEnum, List<Map.Entry<String, String>>> getResultBucket(String sampleName) {
        return buffer
                .computeIfAbsent(
                        sampleName,
                        k -> new ConcurrentHashMap<>()
                );
    }

}
