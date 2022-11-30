package org.apache.jmeter.visualizers.backend.influxdb2.container;

import org.junit.jupiter.api.Test;

import static org.apache.jmeter.visualizers.backend.influxdb2.container.StatisticCounter.PREALLOCATED_VALUES_DEFAULT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

class StatisticCounterTest {

    @Test
    void add() {
        StatisticCounter counter = new StatisticCounter();

        long max = PREALLOCATED_VALUES_DEFAULT * 2L;
        long sum = 0;

        for (long i = 1; i <= max; i++) {
            counter.add(i);
            sum += i;
        }
        assertThat(
                counter.getSamples(),
                is(max)
        );
        assertThat(
                counter.getAverage(),
                is(PREALLOCATED_VALUES_DEFAULT + 0.5f)
        );
        assertThat(
                counter.getMax(),
                is(max)
        );
        assertThat(
                counter.getMin(),
                is(1L)
        );
        assertThat(
                counter.getSum(),
                is(sum)
        );
        assertThat(
                counter.getPercentile(50),
                is(PREALLOCATED_VALUES_DEFAULT + 1L)
        );
        assertThat(
                counter.getPercentile(99),
                is((long) Math.floor(max * 0.99 + 1))
        );
    }

}