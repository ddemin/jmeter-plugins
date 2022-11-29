package org.apache.jmeter.visualizers.backend.influxdb2.container;

import java.util.Arrays;

public class StatisticCounter {

    public static int preallocated = 10_000;

    private long[] data = new long[preallocated];
    private boolean isSorted = false;
    private int size = 0;
    private long sum = 0;

    public synchronized void add(long newItem) {
        if (size >= data.length) {
            System.arraycopy(data, 0, new long[data.length + preallocated], 0, data.length);
        }
        sum += newItem;
        isSorted = false;
        data[size++] = newItem;
    }

    public long getSize() {
        return size;
    }

    public long getSum() {
        return sum;
    }

    public float getAverage() {
        assertStorageIsNotEmpty();
        return sum / (float) size;
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

    public void clear() {
        data = new long[preallocated];
        isSorted = false;
        sum = 0;
        size = 0;
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
