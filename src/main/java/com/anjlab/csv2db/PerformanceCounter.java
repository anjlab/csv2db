package com.anjlab.csv2db;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.time.DurationFormatUtils;

public class PerformanceCounter
{
    private final AtomicLong linesEnqueued = new AtomicLong(0);

    public PerformanceCounter()
    {
        final long delay = 0;
        final long period = TimeUnit.SECONDS.toMillis(3);

        new Timer(true).schedule(new TimerTask()
        {
            private long firstTime = System.currentTimeMillis();
            private long lastTime = firstTime;
            private long lastCount = linesEnqueued.get();

            @Override
            public void run()
            {
                long currentTime = System.currentTimeMillis();
                long currentCount = linesEnqueued.get();

                long totalCount = currentCount - lastCount;
                long totalTime = currentTime - lastTime;

                double linesPerSecond = totalCount * 1d / (totalTime / 1000);

                System.out.print(String.format(
                        "\r%15d lines processed, %10.0f lines/sec, %15s elapsed",
                        currentCount,
                        linesPerSecond,
                        DurationFormatUtils.formatDurationHMS(currentTime - firstTime)));

                lastTime = currentTime;
                lastCount = currentCount;
            }
        }, delay, period);
    }

    public void lineEnqueued()
    {
        linesEnqueued.incrementAndGet();
    }
}
