package org.apache.jmeter.visualizers.backend.reporter.container;

import java.util.Arrays;

public class StatisticCounter {

    static int PREALLOCATED_VALUES_DEFAULT = 1_000;

    private long[] data = new long[PREALLOCATED_VALUES_DEFAULT];
    private boolean isSorted = false;
    private int size = 0;
    // TODO Still looks like a hack. Need to think about this.
    private long samples = Long.MIN_VALUE;
    private long sum = 0;

    public StatisticCounter() {
        clear();
    }

    public synchronized void add(long newItem, long samples) {
        add(newItem);
        addSamples(samples);
    }

    public synchronized void add(long newItem) {
        if (size + 1 >= data.length) {
            long[] extendedData = new long[data.length + PREALLOCATED_VALUES_DEFAULT];
            System.arraycopy(data, 0, extendedData, 0, data.length);
            data = extendedData;
        }
        sum += newItem;
        isSorted = false;
        data[size++] = newItem;
    }

    public synchronized void clear() {
        data = new long[PREALLOCATED_VALUES_DEFAULT];
        isSorted = false;
        size = 0;
        samples = Long.MIN_VALUE;
        sum = 0;
    }

    public long getSamples() {
        return samples == Long.MIN_VALUE ? size : samples;
    }

    public long getSum() {
        return sum;
    }

    public float getAverage() {
        assertStorageIsNotEmpty();
        return sum / (float) getSamples();
    }

    public long getMax() {
        assertStorageIsNotEmpty();
        sortStorageIfNot();
        return data[size - 1];
    }

    public long getMin() {
        assertStorageIsNotEmpty();
        sortStorageIfNot();
        return data[0];
    }

    public long getPercentile(int level) {
        assertStorageIsNotEmpty();
        sortStorageIfNot();
        return data[(int) Math.floor((level / 100.0) * size)];
    }

    private void addSamples(long samples) {
        if (this.samples == Long.MIN_VALUE) {
            this.samples = 0;
        }

        this.samples += samples;
    }

    private void sortStorageIfNot() {
        if (!isSorted) {
            sort();
        }
    }

    private void assertStorageIsNotEmpty() {
        if (size <= 0) {
            throw new IllegalStateException("Percentile calculation error: Statistic storage is empty");
        }
    }

    private void sort() {
        isSorted = true;
        Arrays.sort(data, 0, size);
    }

}
