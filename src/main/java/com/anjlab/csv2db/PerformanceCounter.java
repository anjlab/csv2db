package com.anjlab.csv2db;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.DurationFormatUtils;

import com.codahale.metrics.Meter;

public class PerformanceCounter
{
    private final Meter linesEnqueued = Import.METRIC_REGISTRY.meter("reader.linesEnqueued");

    public PerformanceCounter()
    {
        final long period = TimeUnit.SECONDS.toMillis(5);
        // Skip first period
        final long delay = period;

        new Timer(true).schedule(new TimerTask()
        {
            private long firstTime = System.currentTimeMillis();

            @Override
            public void run()
            {
                long currentTime = System.currentTimeMillis();

                System.out.print(String.format(
                        "\r%15d lines processed, %10.0f lines/second, %15s elapsed",
                        linesEnqueued.getCount(),
                        linesEnqueued.getOneMinuteRate(),
                        DurationFormatUtils.formatDurationHMS(currentTime - firstTime)));
            }
        }, delay, period);
    }

    public void lineEnqueued()
    {
        linesEnqueued.mark();
    }
}
