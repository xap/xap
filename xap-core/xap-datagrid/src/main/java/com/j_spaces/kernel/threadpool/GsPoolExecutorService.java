package com.j_spaces.kernel.threadpool;

import java.util.concurrent.ExecutorService;

public interface GsPoolExecutorService extends ExecutorService {
    int getActiveCount();

    int getQueueSize();

    long getCompletedTaskCount();
}
