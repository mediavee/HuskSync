package net.william278.husksync.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NamedThreadPoolFactory {

    public static ExecutorService newThreadPool(String name, int poolSize) {
        return Executors.newFixedThreadPool(poolSize, new ThreadFactoryBuilder().setNameFormat(name + "-%d").build());
    }
}
