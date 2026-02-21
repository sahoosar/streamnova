package com.di.streamnova.util;

import org.slf4j.MDC;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * Propagates SLF4J MDC (e.g. {@code txId} from {@link com.di.streamnova.aspect.ProjectTransactionTrackingAspect})
 * to child threads so that logs from worker threads are correlated with the same request.
 * <p>
 * MDC is thread-local; without propagation, logs from {@code ExecutorService}, {@code CompletableFuture},
 * or {@code @Async} methods do not contain the request's correlation ID.
 * <p>
 * Usage:
 * <ul>
 *   <li>Wrap before submitting: {@code executor.submit(MdcPropagation.runWithTxId(() -> doWork()));} or {@code executor.submit(MdcPropagation.callWithTxId(() -> doWork()));}</li>
 *   <li>Or wrap the executor once: {@code ExecutorService withMdc = MdcPropagation.wrapExecutor(myExecutor);}</li>
 *   <li>Restore context on a worker: {@code MdcPropagation.runWithMdcContext(savedContext, () -> process(item));}</li>
 * </ul>
 */
public final class MdcPropagation {

    private MdcPropagation() {
    }

    /**
     * Captures the current thread's MDC and returns a Runnable that, when run (e.g. on another thread),
     * sets that MDC for the duration of the task and clears it in {@code finally}.
     * Use when submitting to an {@link ExecutorService}, or passing to {@link CompletableFuture#runAsync(Runnable, java.util.concurrent.Executor)}.
     *
     * @param task the task to run in the child thread
     * @return a runnable that propagates MDC then runs the task
     */
    public static Runnable wrapRunnable(Runnable task) {
        Map<String, String> contextMap = copyMdc();
        return () -> {
            setMdc(contextMap);
            try {
                task.run();
            } finally {
                clearMdc(contextMap);
            }
        };
    }

    /**
     * Captures the current thread's MDC and returns a Callable that, when call() is invoked (e.g. on another thread),
     * sets that MDC for the duration of the task and clears it in {@code finally}.
     *
     * @param task the task to run in the child thread
     * @return a callable that propagates MDC then runs the task
     */
    public static <T> Callable<T> wrapCallable(Callable<T> task) {
        Map<String, String> contextMap = copyMdc();
        return () -> {
            setMdc(contextMap);
            try {
                return task.call();
            } finally {
                clearMdc(contextMap);
            }
        };
    }

    /**
     * Same as {@link #wrapRunnable(Runnable)}: returns a Runnable that propagates current MDC (including {@code txId}) when run on another thread.
     */
    public static Runnable runWithTxId(Runnable task) {
        return wrapRunnable(task);
    }

    /**
     * Same as {@link #wrapCallable(Callable)}: returns a Callable that propagates current MDC (including {@code txId}) when run on another thread.
     */
    public static <T> Callable<T> callWithTxId(Callable<T> task) {
        return wrapCallable(task);
    }

    /**
     * Runs the given task in the current thread with the current MDC (no propagation).
     * Use when the caller will run the returned Runnable on another thread:
     * {@code otherThread.submit(MdcPropagation.runWithMdc(() -> ...));} is wrong — use {@link #wrapRunnable(Runnable)} instead.
     * This method is for the case where you want to ensure MDC is set before running something in the same thread (e.g. after restoring from a stored context).
     *
     * @param task task to run
     */
    public static void runWithMdc(Runnable task) {
        task.run();
    }

    /**
     * Runs the given callable in the current thread. Same as calling {@code task.call()} — provided for API symmetry.
     * For child-thread correlation, use {@link #wrapCallable(Callable)} and submit the result to an executor.
     *
     * @param task task to run
     * @return the result of the callable
     * @throws Exception if the callable throws
     */
    public static <T> T callWithMdc(Callable<T> task) throws Exception {
        return task.call();
    }

    /**
     * Runs the given task in the current thread, with the provided context set in MDC for the duration.
     * Use when you have a stored context (e.g. from a queue) and are about to process it on a worker thread.
     *
     * @param contextMap MDC key-value map (e.g. from a previous {@link #copyMdc()} or custom map with at least {@code txId})
     * @param task       task to run
     */
    public static void runWithMdcContext(Map<String, String> contextMap, Runnable task) {
        setMdc(contextMap);
        try {
            task.run();
        } finally {
            clearMdc(contextMap);
        }
    }

    /**
     * Runs the given callable with the provided context set in MDC for the duration.
     *
     * @param contextMap MDC key-value map
     * @param task       task to run
     * @return the result of the callable
     * @throws Exception if the callable throws
     */
    public static <T> T callWithMdcContext(Map<String, String> contextMap, Callable<T> task) throws Exception {
        setMdc(contextMap);
        try {
            return task.call();
        } finally {
            clearMdc(contextMap);
        }
    }

    /**
     * Returns an executor that wraps every submitted Runnable and Callable with MDC propagation from the submitting thread.
     * Use this when you control the executor (e.g. a shared thread pool); then all {@code execute}/{@code submit} calls
     * automatically carry the current request's {@code txId} (and any other MDC keys) into the worker threads.
     *
     * @param delegate the underlying executor
     * @return an executor that propagates MDC for each task
     */
    public static ExecutorService wrapExecutor(ExecutorService delegate) {
        return new MdcPropagatingExecutor(delegate);
    }

    /**
     * Returns a copy of the current thread's MDC context map, or an empty map if none.
     * Useful when passing context across threads (e.g. to a queue) and restoring later with {@link #runWithMdcContext(Map, Runnable)}.
     *
     * @return copy of MDC context; never null
     */
    public static Map<String, String> copyMdc() {
        Map<String, String> map = MDC.getCopyOfContextMap();
        return map == null ? Collections.emptyMap() : map;
    }

    private static void setMdc(Map<String, String> contextMap) {
        if (contextMap != null && !contextMap.isEmpty()) {
            contextMap.forEach(MDC::put);
        }
    }

    private static void clearMdc(Map<String, String> contextMap) {
        if (contextMap != null && !contextMap.isEmpty()) {
            contextMap.keySet().forEach(MDC::remove);
        }
    }

    private static final class MdcPropagatingExecutor extends java.util.concurrent.AbstractExecutorService {
        private final ExecutorService delegate;

        MdcPropagatingExecutor(ExecutorService delegate) {
            this.delegate = delegate;
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(wrapRunnable(command));
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public java.util.List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, java.util.concurrent.TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }
    }
}
