package com.anjlab.csv2db;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.Timer;

public class SharedBlockingQueueMediator implements Mediator
{
    private final int[] deadRouterProducers;

    private final BlockingQueue<Map<String, Object>>[] routerQueues;
    private final BlockingQueue<String[]> queue;

    private final Map<String, Object> terminalNameValues;
    private final String terminalMessage;

    private final Timer queuePuts;
    private final Timer queueTakes;

    private final Timer[] routerQueuePuts;
    private final Timer[] routerQueueTakes;

    @SuppressWarnings("unchecked")
    public SharedBlockingQueueMediator(Configuration config, int numberOfThreads)
    {
        // Each thread will take batch of lines with the size of batchSize from the queue,
        // that's why it's necessary to always have enough lines for those who read from its thread
        queue = new ArrayBlockingQueue<String[]>(config.getBatchSize() * numberOfThreads);

        terminalNameValues = new HashMap<>();
        terminalMessage = UUID.randomUUID().toString();

        deadRouterProducers = new int[numberOfThreads];
        Arrays.fill(deadRouterProducers, 0);

        if (config.isIgnoreDuplicatePK())
        {
            routerQueues = new BlockingQueue[numberOfThreads];

            if (Import.isMetricsEnabled())
            {
                routerQueuePuts = new Timer[numberOfThreads];
                routerQueueTakes = new Timer[numberOfThreads];
            }
            else
            {
                routerQueuePuts = null;
                routerQueueTakes = null;
            }

            for (int i = 0; i < numberOfThreads; i++)
            {
                // This is likely not the best estimation of router's queue size,
                // but we need to limit it with some value
                routerQueues[i] = new ArrayBlockingQueue<>(config.getBatchSize() * numberOfThreads);

                if (Import.isMetricsEnabled())
                {
                    routerQueuePuts[i] = Import.METRIC_REGISTRY.timer("mediator.router." + i + ".queue.puts");
                    routerQueueTakes[i] = Import.METRIC_REGISTRY.timer("mediator.router." + i + ".queue.takes");

                    final int threadId = i;
                    Import.registerMetric("mediator.router." + i + ".queue.ratio", new RatioGauge()
                    {
                        @Override
                        protected Ratio getRatio()
                        {
                            return Ratio.of(
                                    routerQueuePuts[threadId].getOneMinuteRate(),
                                    routerQueueTakes[threadId].getOneMinuteRate());
                        }
                    });
                }
            }
        }
        else
        {
            routerQueues = null;
            routerQueuePuts = null;
            routerQueueTakes = null;
        }

        if (Import.isMetricsEnabled())
        {
            queuePuts = Import.METRIC_REGISTRY.timer("mediator.queue.puts");
            queueTakes = Import.METRIC_REGISTRY.timer("mediator.queue.takes");
            Import.registerMetric("mediator.queue.ratio", new RatioGauge()
            {
                @Override
                protected Ratio getRatio()
                {
                    return Ratio.of(queuePuts.getOneMinuteRate(), queueTakes.getOneMinuteRate());
                }
            });
        }
        else
        {
            queuePuts = null;
            queueTakes = null;
        }
    }

    @Override
    public void dispatch(String[] line) throws InterruptedException
    {
        Import.measureTime(queuePuts, new VoidCallable<InterruptedException>()
        {
            @Override
            public void run() throws InterruptedException
            {
                queue.put(line);
            }
        });
    }

    @Override
    public void producerDone() throws InterruptedException
    {
        Import.measureTime(queuePuts, new VoidCallable<InterruptedException>()
        {
            @Override
            public void run() throws InterruptedException
            {
                queue.put(new String[] { terminalMessage });
            }
        });
    }

    @Override
    public Object take(int forThreadId) throws InterruptedException
    {
        if (isInTerminalPhase(forThreadId))
        {
            return terminalPhaseTake(forThreadId);
        }

        while (routerQueueHasData(forThreadId))
        {
            Object nameValues = takeFromRouter(forThreadId);

            if (nameValues == terminalNameValues)
            {
                deadRouterProducers[forThreadId]++;
            }
            else
            {
                return nameValues;
            }
        }

        String[] line = Import.measureTime(queueTakes, new Callable<String[]>()
        {
            @Override
            public String[] call() throws InterruptedException
            {
                return queue.take();
            }
        });

        if (!isTerminalLine(line))
        {
            return line;
        }

        // Let other consumers know that producer has finished reading lines,
        // and there won't be new records in the shared queue
        producerDone();

        if (isRouterEnabled())
        {
            deadRouterProducers[forThreadId]++;

            // Notify other consumers that this thread has done processing shared queue,
            // and is waiting for confirmation from other consumers that they don't have any
            // messages for it
            for (int i = 0; i < routerQueues.length; i++)
            {
                if (i != forThreadId)
                {
                    dispatch(terminalNameValues, i);
                }
            }

            return terminalPhaseTake(forThreadId);
        }

        // Empty array is a terminal line
        return new String[0];
    }

    private boolean isInTerminalPhase(int threadId)
    {
        return deadRouterProducers[threadId] > 0;
    }

    private Object takeFromRouter(int forThreadId)
    {
        Timer timer = routerQueueTakes == null
                ? null
                : routerQueueTakes[forThreadId];

        return Import.measureTime(timer, new Callable<Object>()
        {
            @Override
            public Object call() throws InterruptedException
            {
                return routerQueues[forThreadId].take();
            }
        });
    }

    private Object terminalPhaseTake(int forThreadId)
    {
        while (deadRouterProducers[forThreadId] < deadRouterProducers.length)
        {
            Object nameValues = takeFromRouter(forThreadId);

            if (nameValues == terminalNameValues)
            {
                deadRouterProducers[forThreadId]++;
            }
            else
            {
                return nameValues;
            }
        }

        // Empty array is a terminal line
        return new String[0];
    }

    private boolean routerQueueHasData(int forThreadId)
    {
        return isRouterEnabled() && !routerQueues[forThreadId].isEmpty();
    }

    private boolean isRouterEnabled()
    {
        return routerQueues != null;
    }

    private boolean isTerminalLine(String[] line)
    {
        return line.length == 0 || terminalMessage.equals((line)[0]);
    }

    @Override
    public void consumerDone(int threadId) throws InterruptedException
    {
        Import.measureTime(queuePuts, new VoidCallable<InterruptedException>()
        {
            @Override
            public void run() throws InterruptedException
            {
                queue.put(new String[] { terminalMessage, String.valueOf(threadId) });
            }
        });
    }

    @Override
    public void dispatch(final Map<String, Object> nameValues, final int forThreadId)
            throws InterruptedException
    {
        Timer timer = routerQueuePuts == null
                ? null
                : routerQueuePuts[forThreadId];

        Import.measureTime(timer, new VoidCallable<InterruptedException>()
        {
            @Override
            public void run() throws InterruptedException
            {
                routerQueues[forThreadId].put(nameValues);
            }
        });
    }
}
