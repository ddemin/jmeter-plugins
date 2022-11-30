package org.apache.jmeter.visualizers.backend.influxdb2.influx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class InfluxDbServiceScheduledTrigger implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDbServiceScheduledTrigger.class);

    private final InfluxDbService influxService;
    private final ScheduledExecutorService scheduler;
    private final long batchingPeriod;

    private ScheduledFuture<?> timerHandle;

    public InfluxDbServiceScheduledTrigger(InfluxDbService influxService, int batchingPeriod) {
        this.influxService = influxService;
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.batchingPeriod = batchingPeriod;
    }

    @Override
    public void run() {
        influxService.sendOperationsMetrics();
        influxService.processRetryQueue();
    }

    public void init() {
        this.timerHandle = scheduler.scheduleAtFixedRate(
                this,
                batchingPeriod,
                batchingPeriod,
                TimeUnit.SECONDS
        );
    }


    public void destroy() {
        try {
            if (timerHandle != null) {
                timerHandle.cancel(false);
                if (scheduler != null) {
                    scheduler.shutdown();
                    try {
                        scheduler.awaitTermination(60, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        LOG.error("Error waiting for end of scheduler");
                        Thread.currentThread().interrupt();
                    }
                }
            }
        } catch (Throwable tr) {
            LOG.error("Something goes wrong during InfluxDB integration teardown: " + tr.getMessage(), tr);
        }
    }

}
