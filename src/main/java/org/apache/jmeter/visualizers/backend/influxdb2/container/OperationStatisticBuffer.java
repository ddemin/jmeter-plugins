package org.apache.jmeter.visualizers.backend.influxdb2.container;

import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.jmeter.visualizers.backend.influxdb2.container.StatisticTypeEnum.*;

public class OperationStatisticBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(OperationStatisticBuffer.class);

    private final Map<String, Map<StatisticTypeEnum, StatisticCounter>> buffer
            = Collections.synchronizedMap(new HashMap<>());

    public void putMetric(SampleResult sampleResult) {
            putLatencyMetrics(sampleResult);
            putLoadMetrics(sampleResult);
            putErrorMetrics(sampleResult);
            putNetworkMetrics(sampleResult);
    }

    public Map<String, Map<StatisticTypeEnum, StatisticCounter>> getBuffer() {
        return buffer;
    }

    public void clear() {
        LOG.debug("Cleanup");
        buffer.clear();
    }

    void putLatencyMetrics(SampleResult sampleResult) {
        getResultBucket(sampleResult)
                .putIfAbsent(LATENCY, new StatisticCounter())
                .add(sampleResult.getLatency());
    }

    void putLoadMetrics(SampleResult sampleResult) {
        getResultBucket(sampleResult)
                .putIfAbsent(LOAD, new StatisticCounter())
                .add(1);

    }

    void putErrorMetrics(SampleResult sampleResult) {
        for (int sampleNum = 0; sampleNum < sampleResult.getSampleCount(); sampleNum++) {
            getResultBucket(sampleResult)
                    .putIfAbsent(ERROR, new StatisticCounter())
                    .add(sampleNum < sampleResult.getErrorCount() ? 1L : 0L);
        }
    }

    void putNetworkMetrics(SampleResult sampleResult) {
        getResultBucket(sampleResult)
                .putIfAbsent(NETWORK, new StatisticCounter())
                .add(sampleResult.getBytesAsLong() + sampleResult.getSentBytes());
    }

    Map<StatisticTypeEnum, StatisticCounter> getResultBucket(SampleResult sampleResult) {
        return buffer
                .putIfAbsent(
                        sampleResult.getSampleLabel(),
                        new ConcurrentHashMap<>()
                );
    }

}
