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
package com.gigaspaces.security.directory;

/**
 * Represents a CredentialsProvider with null UserDetails.
 * usage: {@code EmptyCredentialsProvider.get();}
 * @Since 14.0
 */
public class EmptyCredentialsProvider extends CredentialsProvider {

    private static final EmptyCredentialsProvider singleton = new EmptyCredentialsProvider();

    //private constructor to avoid client applications to use constructor
    private EmptyCredentialsProvider(){}

    @Override
    public UserDetails getUserDetails() {
        return null;
    }

    /**
     * @return a singleton instance of this class
     */
    public static EmptyCredentialsProvider get() {
        return singleton;
    }
}
