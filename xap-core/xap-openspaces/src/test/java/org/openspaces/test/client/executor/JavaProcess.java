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

/*
 * @(#)JavaProcess.java   Jul 19, 2007
 *
 * Copyright 2007 GigaSpaces Technologies Inc.
 */
package org.openspaces.test.client.executor;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * JavaProcess provides lifecycle of forkable JVM process defined by {@link JavaCommand}.
 * JavaProcess has a WatchDog capabilities which protects zombi JavaProcess without parent JVM. If
 * parent JVM of this JavaProcess was destroyed, this JVM will be destroyed as well by
 * JavaProcessWatchDog.<br> The Watchdog periodically every 15 sec checks liveness of parent JVM.
 *
 * NOTE: JavaProcess implementation must not have any dependences with TGrid runtime libraries and
 * {@link Logger}.
 *
 * @author Igor Goldenberg
 * @see JavaCommand
 * @since 1.0
 **/
public class JavaProcess
        implements IJavaProcess {
    protected final ProcessArgs _procArg;

    /**
     * JavaProcess constructor
     *
     * @param args target class main arguments
     **/
    protected JavaProcess(String[] args) {
        _procArg = new ProcessArgs(args);
    }

    /**
     * @return parsed process arguments.
     */
    protected ProcessArgs getProcessArgs() {
        return _procArg;
    }

    /**
     * carry out the main sequence - 'public static void main(String[] args)'
     */
    private void callMain() throws ForkProcessException {

        try {
            Object objectParameters[] = {getProcessArgs().getMainArgs()};
            Class<?> classParameters[] = {objectParameters[0].getClass()};

            Class<?> c = Class.forName(_procArg.getMainClass());
            Method m = c.getDeclaredMethod("main", classParameters);
            m.invoke(null, objectParameters); //call static method (obj argument is ignored - thus null)
        } catch (InvocationTargetException invte) {
            throw newForkProcessException(invte.getTargetException());
        } catch (Throwable t) {
            throw newForkProcessException(t);
        }
    }

    /**
     * builds a fork process exception
     */
    protected ForkProcessException newForkProcessException(Throwable t) {
        return new ForkProcessException("Failed to start process [" + getProcessArgs() + "]", t);
    }

    /**
     * 1. binds the process to the registry <p> 2. call the process main method <p> <p> Note: It
     * must happen in this order so that the process can be looked-up, while it's main is executing
     * (for example by the watchdog).
     */
    protected void run() throws Exception {
        bindToProcessRegistry();
        callMain();
    }

    /**
     * the main method to initialize JavaProcess
     */
    public static void main(String... args) {
        System.setErr(System.out);

        ProcessLogger.log("JavaProcess - Starting with args: " + Arrays.asList(args));

        try {
          /* bind JavaProcess to ExecuterRegistry */
            JavaProcess jp = new JavaProcess(args);
            jp.run();

            //allow daemon threads to keep this VM from terminating
        } catch (Throwable t) {
            //nowhere to throw - it will be uncaught - signal by exiting with error code 1
            ProcessLogger.log("JavaProcess - Termination was caused by: ", t);
            System.exit(1);
        }
    }


    /**
     * process argument parser
     */
    final static protected class ProcessArgs {
        int port;
        String execUrl;
        String mainClass;
        String[] mainArgs;

        ProcessArgs(String[] args) {
            mainArgs = args;
            execUrl = System.getProperty(JavaCommand.PROCESS_UID);
            mainClass = System.getProperty(JavaCommand.MAIN_CLASS);

            String[] splitArgs = execUrl.split("@");
            port = Integer.parseInt(splitArgs[1]);
        }

        int getPort() {
            return port;
        }

        String getMainClass() {
            return mainClass;
        }

        String[] getMainArgs() {
            return mainArgs;
        }

        String getExecuterURL() {
            return execUrl;
        }

        @Override
        public String toString() {
            return "[ mainClass=" + mainClass
                    + "\n mainArgs=" + Arrays.asList(mainArgs)
                    + "\n execUrl=" + execUrl
                    + "\n port=" + port
                    + "]";

        }
    }

    /**
     * Start JavaProcess, export IJavaProcess as remoteObject and bind to Executer registry.
     */
    protected void bindToProcessRegistry() throws RemoteException {

        ProcessLogger.log("Locating registry on port: " + _procArg.getPort());

        ProcessRegistry processRegistry = ProcessRegistry.locateRegistry(_procArg.getPort());

        ProcessLogger.log("Located ProcessRegistry: " + processRegistry
                + "\n\t -> Registering with executerURL:  [JNDI URL=" + _procArg.getExecuterURL() + "]");

        processRegistry.register(_procArg.getExecuterURL(), getProcessStub());

        ProcessLogger.log(" -> Registered.");

        processRegistry.monitor(_procArg.getExecuterURL());
    }

    /**
     * @return the process stub of this java process to bind to RMIRegistry
     */
    protected Remote getProcessStub()
            throws RemoteException {
        return new WatchableEntry();
    }

    /**
     * dummy watchable entry for WatchDog
     */
    final static private class WatchableEntry
            implements Remote, Serializable {
        private static final long serialVersionUID = 1L;
    }
}
