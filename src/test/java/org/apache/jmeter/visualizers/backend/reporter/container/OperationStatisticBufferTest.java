package org.apache.jmeter.visualizers.backend.reporter.container;

import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.samplers.StatisticalSampleResult;
import org.hamcrest.core.Is;
import org.junit.jupiter.api.Test;

import static org.apache.jmeter.visualizers.backend.reporter.container.StatisticTypeEnum.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;

class OperationStatisticBufferTest {

    @Test
    void putMetric() {
        OperationStatisticBuffer buffer = new OperationStatisticBuffer();

        StatisticalSampleResult someSample = new StatisticalSampleResult(System.currentTimeMillis(), 1234L);
        String label1 = "service1: operation 1";
        someSample.setSampleLabel(label1);
        someSample.setSampleCount(10);
        someSample.setSuccessful(false);
        someSample.setErrorCount(5);
        someSample.setBytes(1000);
        someSample.setSentBytes(1000);

        buffer.putMetric(someSample);
        buffer.putMetric(someSample);


        StatisticalSampleResult someSample2 = new StatisticalSampleResult(System.currentTimeMillis(), 2345L);
        String label2 = "service2: operation 2";
        someSample2.setSampleLabel(label2);
        someSample2.setSampleCount(20);
        someSample2.setSuccessful(false);
        someSample2.setErrorCount(5);
        someSample2.setBytes(2000);
        someSample2.setSentBytes(2000);
        buffer.putMetric(someSample2);

        assertThat(
                buffer.getBuffer(),
                aMapWithSize(2)
        );
        assertThat(
                buffer.getBuffer().get(label1),
                aMapWithSize(4)
        );
        assertThat(
                buffer.getBuffer().get(label1).get(LATENCY).getAverage(),
                Is.is(1234.0f)
        );
        assertThat(
                buffer.getBuffer().get(label1).get(ERROR).getSum(),
                Is.is(10L)
        );
        assertThat(
                buffer.getBuffer().get(label1).get(NETWORK).getSum(),
                Is.is(4000L)
        );
        assertThat(
                buffer.getBuffer().get(label1).get(LOAD).getSum(),
                Is.is(20L)
        );

        assertThat(
                buffer.getBuffer().get(label2).get(LATENCY).getAverage(),
                Is.is(2345.0f)
        );
        assertThat(
                buffer.getBuffer().get(label2).get(ERROR).getMin(),
                Is.is(5L)
        );
        assertThat(
                buffer.getBuffer().get(label2).get(NETWORK).getSum(),
                Is.is(4000L)
        );
        assertThat(
                buffer.getBuffer().get(label2).get(LOAD).getSum(),
                Is.is(20L)
        );
    }

    @Test
    void clear() {
        OperationStatisticBuffer buffer = new OperationStatisticBuffer();

        SampleResult someSample = new SampleResult(System.currentTimeMillis(), 1234L);
        someSample.setSampleLabel("service1: operation 1");
        someSample.setSuccessful(false);
        someSample.setResponseCode("504");
        buffer.putMetric(someSample);

        assertThat(
                buffer.getBuffer(),
                aMapWithSize(1)
        );

        buffer.getBuffer().clear();
        assertThat(
                buffer.getBuffer(),
                aMapWithSize(0)
        );
        buffer.clear();
        assertThat(
                buffer.getBuffer(),
                aMapWithSize(0)
        );
    }
}