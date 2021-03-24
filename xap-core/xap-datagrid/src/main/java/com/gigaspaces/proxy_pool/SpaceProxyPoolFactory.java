package com.gigaspaces.proxy_pool;

import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.driver.GConnection;

import java.sql.SQLException;
import java.util.Properties;

public interface SpaceProxyPoolFactory {

     GConnection createClonedProxy(IJSpace space, Properties props) throws SQLException;

    }
