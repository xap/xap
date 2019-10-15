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

package com.gigaspaces.lrmi.classloading;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * the remote interface used to retsrieve class definition or resources from a remote JVM.
 *
 * @author asy ronen
 * @since 6.6
 */
public interface IClassProvider extends Remote {

    //TODO expose different interface for local interaction and for remote interaction, remote invocation should
    //only call getClassDefinition and getResource

    byte[] getClassDefinition(long id, String className) throws RemoteException, ClassNotFoundException;

    byte[] getResource(long id, String resourceName) throws RemoteException;

    long putClassLoader(ClassLoader classLoader) throws RemoteException;
}
