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
package com.gigaspaces.persistency;

import com.gigaspaces.datasource.SpaceDataSource;
import com.mongodb.MongoClient;
import com.gigaspaces.datasource.SpaceDataSourceFactory;

/**
 * @author Alon Shoham
 * @since 15.5.0
 */
public class MongoSpaceDataSourceFactory implements SpaceDataSourceFactory {
    private static final long serialVersionUID = 2696958279935086850L;
    private String _db,_host;
    private int _port;

    public MongoSpaceDataSourceFactory() {
    }

    public String getDb() {
        return _db;
    }

    public void setDb(String db) {
        this._db = db;
    }

    public String getHost() {
        return _host;
    }

    public void setHost(String host) {
        this._host = host;
    }

    public int getPort() {
        return _port;
    }

    public void setPort(int port) {
        this._port = port;
    }

    public SpaceDataSource create() {
        MongoClient mongoClient = new MongoClient(_host,_port);
        MongoClientConnector mongoClientConnector = new MongoClientConnector(mongoClient, _db, false);
        return new MongoSpaceDataSource(mongoClientConnector, null);
    }
}
