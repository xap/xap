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
