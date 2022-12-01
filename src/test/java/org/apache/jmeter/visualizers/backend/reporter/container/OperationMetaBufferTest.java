package org.apache.jmeter.visualizers.backend.reporter.container;

import org.apache.jmeter.samplers.SampleResult;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;

import static org.apache.jmeter.visualizers.backend.reporter.util.Utils.UNDEFINED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class OperationMetaBufferTest {

    @Test
    void putLabelsMeta() {
        OperationMetaBuffer buffer = new OperationMetaBuffer();

        SampleResult sample = new SampleResult(System.currentTimeMillis(), 1234L);
        String label = "service1: operation 1";
        sample.setSampleLabel(label);
        buffer.putLabelsMeta(sample, " some label : some value , just label ,,");


        assertThat(
                buffer.getBuffer(),
                aMapWithSize(1)
        );
        assertThat(
                buffer.getBuffer().get(label),
                aMapWithSize(1)
        );
        assertThat(
                buffer.getBuffer().get(label).get(MetaTypeEnum.LABEL),
                hasSize(2)
        );

        assertThat(
                buffer.getBuffer().get(label).get(MetaTypeEnum.LABEL).get(0),
                equalTo(new AbstractMap.SimpleEntry<>("some label", "some value"))
        );
        assertThat(
                buffer.getBuffer().get(label).get(MetaTypeEnum.LABEL).get(1),
                equalTo(new AbstractMap.SimpleEntry<>("just label", UNDEFINED))
        );
    }

}