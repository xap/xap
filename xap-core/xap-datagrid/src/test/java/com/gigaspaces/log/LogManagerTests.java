/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gigaspaces.log;

import com.gigaspaces.logger.GSLogConfigLoader;
import com.j_spaces.kernel.ResourceLoader;
import com.j_spaces.kernel.SystemProperties;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

@com.gigaspaces.api.InternalApi
public class LogManagerTests {

    private static final int nThreads = 12;

    @Test
    public void testDeadlock() throws Exception {

        ClassLoader classLoader = ResourceLoader.class.getClassLoader();
        URL url = classLoader.getResource(SystemProperties.GS_LOGGING_CONFIG_FILE_PATH);
        String path = url.getPath();
        Assert.assertNotNull(path);

        System.setProperty("java.util.logging.config.file", path);

        final CountDownLatch latch = new CountDownLatch(nThreads);
        ExecutorService e = Executors.newFixedThreadPool(nThreads);
        Collection<Callable<Object>> tasks = new ArrayList<Callable<Object>>();

        for (int i = 0; i < nThreads; ++i) {
            final int index = i;
            Callable<Object> c = new Callable<Object>() {

                @Override
                public Object call() throws Exception {
                    System.out.println("running Logger.getLogger(mylogger-" + index + ")");
                    Logger mylogger = Logger.getLogger("mylogger-" + index);
                    mylogger.info("bar-" + index);
                    latch.countDown();
                    return null;
                }
            };
            tasks.add(c);
        }

        e.invokeAll(tasks, 10, TimeUnit.SECONDS);
        Assert.assertEquals("possible deadlock? only " + latch.getCount() + " threads returned, out of " + nThreads, 0L, latch.getCount());
    }

//    @Test
//    public void testOverrides() throws Exception {
//
//        final String logger1Name = "com.gigaspaces.client";
//        final Level logger1Level = Level.FINE;
//        System.setProperty(logger1Name + ".level", logger1Level.toString());
//        testLogger(logger1Name, logger1Level);
//
//        final String logger2Name = "com.gigaspaces.start";
//        final Level logger2Level = Level.FINEST;
//
//        File ext = new File("./log/xap_logging_ext.properties");
//        System.setProperty("java.util.logging.config.file", ext.getAbsolutePath());
//        try {
//            FileWriter fw = new FileWriter(ext);
//            fw.write(logger2Name + ".level=" + logger2Level.toString());
//            fw.close();
//
//            testLogger(logger2Name, logger2Level);
//        } finally {
//            ext.deleteOnExit();
//            ext.delete();
//        }
//    }

    private static void testLogger(String name, Level expectedLevel) {
        //turn on logging output
        System.setProperty("com.gigaspaces.logging.level.config", "true");
        GSLogConfigLoader.reset();
        GSLogConfigLoader.getLoader();
        LogManager logManager = LogManager.getLogManager();
        String property = logManager.getProperty(name + ".level");
        Assert.assertEquals(expectedLevel.toString(), property);

        Logger logger = Logger.getLogger(name);
        Assert.assertEquals(expectedLevel, logger.getLevel());
    }
}
