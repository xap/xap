package com.gigaspaces.logger;

import com.gigaspaces.api.InternalApi;
import org.slf4j.Logger;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * @author Niv Ingberg
 * @since 15.0
 */
@InternalApi
public class ActivityLogger {
    private final String action;
    private final Logger logger;
    private final boolean logFailsAsErrors;
    private final int consecutiveFailuresThreshold;
    private final int consecutiveFailuresLogRatio;
    private final State state;

    private ActivityLogger(Builder builder) {
        this.action = builder.action;
        this.logger = builder.logger;
        this.logFailsAsErrors = builder.logFailsAsErrors;
        this.consecutiveFailuresThreshold = builder.consecutiveFailuresThreshold;
        this.consecutiveFailuresLogRatio = builder.consecutiveFailuresLogRatio;
        this.state = builder.concurrent ? new ConcurrentState(builder) : new NonConcurrentState(builder);
    }

    public void success() {
        state.disableInitialTimer();
        long consecutive = state.resetFailures();
        if (consecutive != 0)
            logger.info("{} succeeded after {} consecutive failures", action, consecutive);
    }

    public void fail(Throwable e) {
        fail(e, (Supplier<String>)null);
    }

    public void fail(Throwable e, String details) {
        fail(e, () -> details);
    }

    public void fail(Throwable e, Supplier<String> details) {
        if (state.isInitialTimerActive()) {
            if (logger.isDebugEnabled())
                logger.debug("{} failed within initial silent duration{}", action, formatDetails(details), e);
            return;
        }

        long consecutive = state.incrementFailures();
        if (consecutive < consecutiveFailuresThreshold) {
            if (isLogFailEnabled())
                logFail("{} failed {} consecutive times{}", action, consecutive, formatDetails(details), e);
        } else if (consecutive == consecutiveFailuresThreshold) {
            if (isLogFailEnabled())
                logFail("{} failed {} consecutive times (activating reduced logging for 1-in-{} failures until problem is resolved){}", action, consecutive, consecutiveFailuresLogRatio, formatDetails(details), e);
        } else {
            if ((consecutive - consecutiveFailuresThreshold) % consecutiveFailuresLogRatio == 0) {
                if (isLogFailEnabled())
                    logFail("{} failed {} consecutive times (reduced logging is active, logging 1-in{} failures){}", action, consecutive, consecutiveFailuresLogRatio, formatDetails(details), e);
            } else {
                if (logger.isDebugEnabled())
                    logger.debug("{} failed {} consecutive times{}", action, consecutive, formatDetails(details), e);
            }
        }
    }

    public boolean isInitialTimerActive() {
        return state.isInitialTimerActive();
    }

    private boolean isLogFailEnabled() {
        return logFailsAsErrors ? logger.isErrorEnabled() : logger.isWarnEnabled();
    }

    private void logFail(String format, Object... arguments) {
        if (logFailsAsErrors) {
            logger.error(format, arguments);
        } else {
            logger.warn(format, arguments);
        }
    }

    private String formatDetails(Supplier<String> detailsSupplier) {
        String details = detailsSupplier != null ? detailsSupplier.get() : null;
        return details == null || details.isEmpty() ? "" : " - " + details;
    }

    private static abstract class State {
        abstract long resetFailures();
        abstract long incrementFailures();
        abstract Timer getInitialTimer();
        abstract void disableInitialTimer();

        boolean isInitialTimerActive() {
            Timer initialTimer = getInitialTimer();
            if (initialTimer == null)
                return false;
            if (initialTimer.isActive())
                return true;
            disableInitialTimer();
            return false;
        }
    }

    private static class NonConcurrentState extends State {
        private long failures;
        private Timer initialTimer;

        private NonConcurrentState(Builder builder) {
            this.initialTimer = builder.initialSilentDuration != null ? new Timer(builder) : null;
        }

        @Override
        public long resetFailures() {
            long result = failures;
            failures = 0;
            return result;
        }

        @Override
        public long incrementFailures() {
            return ++failures;
        }

        @Override
        public Timer getInitialTimer() {
            return initialTimer;
        }

        @Override
        public void disableInitialTimer() {
            initialTimer = null;
        }
    }

    private static class ConcurrentState extends State {
        private final AtomicLong failures = new AtomicLong();
        private final boolean initialTimerConfigured;
        private final AtomicReference<Timer> initialTimer;

        private ConcurrentState(Builder builder) {
            this.initialTimerConfigured = builder.initialSilentDuration != null;
            this.initialTimer = builder.initialSilentDuration != null ? new AtomicReference<>(new Timer(builder)) : null;
        }

        @Override
        public long resetFailures() {
            return failures.getAndSet(0);
        }

        @Override
        public long incrementFailures() {
            return failures.incrementAndGet();
        }

        @Override
        public Timer getInitialTimer() {
            return initialTimerConfigured ? initialTimer.get() : null;
        }

        @Override
        public void disableInitialTimer() {
            if (initialTimerConfigured) {
                this.initialTimer.set(null);
                // TODO: is lazySet safe here? initialTimer.lazySet(null);
            }
        }
    }

    private static class Timer {
        private final Clock clock;
        private final long deadline;

        private Timer(Builder builder) {
            this.clock = builder.clock;
            this.deadline = clock.millis() + builder.initialSilentDuration.toMillis();
        }

        private boolean isActive() {
            return clock.millis() < deadline;
        }
    }

    public static class Builder {
        private final String action;
        private final Logger logger;
        private int consecutiveFailuresThreshold = 10;
        private int consecutiveFailuresLogRatio = 100;
        private Duration initialSilentDuration;
        private boolean logFailsAsErrors;
        private boolean concurrent;
        private Clock clock = Clock.systemUTC();

        public Builder(String action, Logger logger) {
            this.action = action;
            this.logger = logger;
        }

        public ActivityLogger build() {
            return new ActivityLogger(this);
        }

        public Builder reduceConsecutiveFailuresLogging(int threshold, int logRatio) {
            this.consecutiveFailuresThreshold = threshold;
            this.consecutiveFailuresLogRatio = logRatio;
            return this;
        }

        public Builder initialSilentDuration(Duration initialSilentDuration) {
            this.initialSilentDuration = initialSilentDuration;
            return this;
        }

        public Builder logFailsAsErrors() {
            this.logFailsAsErrors = true;
            return this;
        }

        public Builder concurrent() {
            this.concurrent = true;
            return this;
        }

        @InternalApi
        Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }
    }
}
