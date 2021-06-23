package com.gigaspaces.jdbc.request;

import com.j_spaces.jdbc.QueryHandler;
import com.j_spaces.jdbc.QuerySession;
import com.j_spaces.jdbc.RequestPacket;
import com.j_spaces.jdbc.ResponsePacket;
import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;
import java.sql.SQLException;

public class RequestPacketV3 extends RequestPacket {
    static final long serialVersionUID = -1867146582296329910L;
    @Override
    public ResponsePacket accept(QueryHandler handler, QuerySession session) throws LeaseDeniedException, RemoteException, TransactionException, SQLException {
        return handler.visit(this, session);
    }
}
