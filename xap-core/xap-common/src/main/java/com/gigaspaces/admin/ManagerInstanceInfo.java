package com.gigaspaces.admin;

import java.io.Serializable;

/**
 * Enacpsulates information about a specific manager instance.
 *
 * @author Niv Ingberg
 * @since 15.5
 */
public interface ManagerInstanceInfo extends Serializable {
    /**
     * Returns the manager's host.
     */
    String getHost();
}
