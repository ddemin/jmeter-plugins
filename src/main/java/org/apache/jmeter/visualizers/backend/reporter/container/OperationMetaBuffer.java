package org.apache.jmeter.visualizers.backend.reporter.container;

import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.jmeter.visualizers.backend.reporter.util.Utils.toMapWithLowerCaseKey;

public class OperationMetaBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(OperationMetaBuffer.class);

    private final Map<String, Map<MetaTypeEnum, List<Map.Entry<String, Object>>>> buffer
            = Collections.synchronizedMap(new HashMap<>());

    public Map<String, Map<MetaTypeEnum, List<Map.Entry<String, Object>>>> getBuffer() {
        return buffer;
    }

    public void putLabelsMeta(String sampleName, String labels) {
        getResultBucket(sampleName)
                .computeIfAbsent(MetaTypeEnum.LABEL, k -> Collections.synchronizedList(new ArrayList<>()))
                .addAll(
                        toMapWithLowerCaseKey(labels).entrySet().stream().toList()
                );
    }

    public void putLabelsMeta(SampleResult sampleResult, String labels) {
        putLabelsMeta(sampleResult.getSampleLabel(), labels);
    }

    Map<MetaTypeEnum, List<Map.Entry<String, Object>>> getResultBucket(String sampleName) {
        return buffer
                .computeIfAbsent(
                        sampleName,
                        k -> new ConcurrentHashMap<>()
                );
    }

    public void clear() {
        buffer.forEach(
                (samplerName, metaMap) ->
                        metaMap.forEach(
                                (type, listOfMeta) -> listOfMeta.clear()
                        )
        );
    }

}
