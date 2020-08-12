package com.gigaspaces.datasource;

import com.gigaspaces.datasource.SpaceDataSource;
import java.io.Serializable;

/**
 * @author Alon Shoham
 * @since 15.5.0
 */
public interface SpaceDataSourceFactory extends Serializable {
    SpaceDataSource create();
}
