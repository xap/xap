package com.gigaspaces.admin.manager;

import com.gigaspaces.admin.ManagerClusterInfo;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * @author Niv Ingberg
 * @since 15.5
 */
public interface ManagerClusterInfoProvider extends Remote {
    ManagerClusterInfo getManagerClusterInfo() throws RemoteException;;
}
