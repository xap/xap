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
