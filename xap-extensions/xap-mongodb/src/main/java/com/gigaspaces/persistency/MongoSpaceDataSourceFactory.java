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

    public MongoSpaceDataSourceFactory db(String db) {
        this._db = db;
        return this;
    }

    public String getHost() {
        return _host;
    }

    public MongoSpaceDataSourceFactory host(String host) {
        this._host = host;
        return this;
    }

    public int getPort() {
        return _port;
    }

    public MongoSpaceDataSourceFactory port(int port) {
        this._port = port;
        return this;
    }

    public SpaceDataSource create() {
        MongoClient mongoClient = new MongoClient(_host,_port);
        MongoClientConnector mongoClientConnector = new MongoClientConnector(mongoClient, _db, false);
        return new MongoSpaceDataSource(mongoClientConnector, null);
    }
}
