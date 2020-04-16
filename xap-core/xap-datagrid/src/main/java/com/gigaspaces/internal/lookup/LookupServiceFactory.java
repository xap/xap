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

package com.gigaspaces.internal.lookup;

import com.sun.jini.reggie.Registrar;

import java.rmi.RemoteException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts Lookup Registry
 *
 * @author evgenyf
 * @since 12.0
 */
@com.gigaspaces.api.InternalApi
public class LookupServiceFactory {

    private static final Logger logger = LoggerFactory.getLogger(LookupServiceFactory.class.getName());

    public static void main(String[] args) {
        try {
            final Registrar registrar = RegistrarFactory.createRegistrar();
            // Use the MAIN thread as the non daemon thread to keep it alive
            final Thread mainThread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        registrar.destroy();
                        mainThread.interrupt();
                    } catch (RemoteException e) {
                        if (logger.isErrorEnabled()) {
                            logger.error(e.toString(), e);
                        }
                    }
                }
            });

            while (!mainThread.isInterrupted()) {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) {
                    // do nothing, simply exit
                }
            }
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error(e.toString(), e);
            }
        }
    }
}
