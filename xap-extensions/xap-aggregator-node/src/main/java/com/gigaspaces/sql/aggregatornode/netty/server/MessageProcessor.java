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
package com.gigaspaces.sql.aggregatornode.netty.server;

import com.gigaspaces.sql.aggregatornode.netty.exception.BreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.NonBreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.authentication.Authentication;
import com.gigaspaces.sql.aggregatornode.netty.authentication.AuthenticationProvider;
import com.gigaspaces.sql.aggregatornode.netty.authentication.ClearTextPassword;
import com.gigaspaces.sql.aggregatornode.netty.query.*;
import com.gigaspaces.sql.aggregatornode.netty.utils.TypeUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;

import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

import static com.gigaspaces.sql.aggregatornode.netty.utils.Constants.*;
import static com.gigaspaces.sql.aggregatornode.netty.utils.TypeUtils.*;

public class MessageProcessor extends ChannelInboundHandlerAdapter {
    private static final AtomicInteger ID_COUNTER = new AtomicInteger();
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MessageProcessor.class);

    private final int id;
    private final int secret;

    private final QueryProvider queryProvider;
    private final AuthenticationProvider authProvider;

    private boolean initRead;
    private Charset charset = DEFAULT_CHARSET;
    private String dateStyle = DEFAULT_DATE_STYLE;
    private TimeZone timeZone = DEFAULT_TIME_ZONE;
    private String username;
    private String database;

    public MessageProcessor(QueryProvider queryProvider, AuthenticationProvider authProvider) {
        this.queryProvider = queryProvider;
        this.authProvider = authProvider;

        id = ID_COUNTER.getAndIncrement();
        secret = new SecureRandom(SecureRandom.getSeed(8)).nextInt();
    }

    /**
     * @return Process id, used for query cancelling.
     */
    public int getId() {
        return this.id;
    }

    /**
     * @return Secret, used for query cancelling.
     */
    public int getSecret() {
        return this.secret;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf msg0 = (ByteBuf) msg;
        try {
            onMessage(ctx, initRead ? (char) msg0.readByte() : 0, msg0.skipBytes(4));
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    void onMessage(ChannelHandlerContext ctx, char type, ByteBuf msg) {
        try {
            switch (type) {
                case 0:
                    onInit(ctx, msg);
                    break;
    
                case 'p':
                    onPassword(ctx, msg);
                    break;
    
                case 'P':
                    onParse(ctx, msg);
                    break;
    
                case 'B':
                    onBind(ctx, msg);
                    break;
    
                case 'D':
                    onDescribe(ctx, msg);
                    break;
    
                case 'E':
                    onExecute(ctx, msg);
                    break;

                case 'C':
                    onClose(ctx, msg);
                    break;

                case 'H':
                    onFlush(ctx, msg);
                    break;

                case 'Q':
                    onQuery(ctx, msg);
                    break;

                case 'X':
                    onTerminate(ctx, msg);
                    break;

                case 'S':
                    onSync(ctx, msg);
                    break;
    
                default:
                    throw new BreakingException("08P01" /* protocol violation */, "unexpected message type");
            }
        } catch (ProtocolException e) {
            e.printStackTrace(System.err);
            log.error("CODE " + e.getCode() + ": " + e.getMessage(), e);
            ByteBuf buf = ctx.alloc().ioBuffer();
            writeError(buf, e.getCode(), e.getMessage());
            if (e.closeSession()) {
                ctx.write(buf);
                ctx.close();
            } else {
                writeReadyForQuery(buf);
                ctx.write(buf);
            }
        }
    }

    private void onSync(ChannelHandlerContext ctx, ByteBuf msg) {
        writeReadyForQuery(ctx);
    }

    private void onTerminate(ChannelHandlerContext ctx, ByteBuf msg) {
        ctx.close();
    }

    private void onQuery(ChannelHandlerContext ctx, ByteBuf msg) throws ProtocolException {
        String query = readString(msg);
        List<String> statements = null;
        ByteBuf buf = null;
        try {
            statements = queryProvider.prepareMultiline(query);
            buf = ctx.alloc().ioBuffer();
            for (String stmt : statements) {
                queryProvider.bind("", stmt, new Object[0], new int[0]);
                Portal<?> portal = queryProvider.execute("");
                if (portal.tag() != Portal.Tag.NONE) {
                    writeRowDescription(buf, portal.getDescription());
                    int inBatch = 0;
                    while (portal.hasNext()) {
                        writeDataRow(buf, portal.next(), portal.getDescription());
                        if (++inBatch == BATCH_SIZE) {
                            ctx.write(buf);
                            buf = ctx.alloc().ioBuffer();
                            inBatch = 0;
                        }
                    }
                    writeCommandComplete(buf, portal.tag(), portal.processed());
                } else
                    writeEmptyResponse(buf);

                queryProvider.closeS(stmt);
            }
            writeReadyForQuery(buf);
            ctx.write(buf);
        } catch (Exception e) {
            ReferenceCountUtil.release(buf);
            if (statements != null) {
                for (String stmt : statements) {
                    try {
                        queryProvider.closeS(stmt);
                    } catch (ProtocolException ex) {
                        e.addSuppressed(ex);
                    }
                }
            }
            throw e;
        }
    }

    private void onFlush(ChannelHandlerContext ctx, ByteBuf msg) {
        ctx.flush();
    }

    private void onClose(ChannelHandlerContext ctx, ByteBuf msg) throws ProtocolException {
        switch (msg.readByte()) {
            case 'S':
                onCloseStatement(ctx, readString(msg));
                break;

            case 'P':
                onClosePortal(ctx, readString(msg));
                break;

            default:
                throw new BreakingException("08P01" /* protocol violation */, "unexpected close type");
        }
    }

    private void onClosePortal(ChannelHandlerContext ctx, String portal) throws ProtocolException {
        queryProvider.closeP(portal);
        writeCloseComplete(ctx);
    }

    private void onCloseStatement(ChannelHandlerContext ctx, String stmt) throws ProtocolException {
        queryProvider.closeS(stmt);
        writeCloseComplete(ctx);
    }

    private void onExecute(ChannelHandlerContext ctx, ByteBuf msg) throws ProtocolException {
        String pName = readString(msg);
        int fetch = msg.readInt();
        ByteBuf buf = null;
        try {
            buf = ctx.alloc().ioBuffer();
            Portal<?> portal = queryProvider.execute(pName);
            if (portal.tag() != Portal.Tag.NONE) {
                int inBatch = 0;
                while (portal.hasNext()) {
                    writeDataRow(buf, portal.next(), portal.getDescription());

                    if (fetch != 0 && --fetch == 0)
                        break;

                    if (++inBatch == BATCH_SIZE) {
                        ctx.write(buf);
                        buf = ctx.alloc().ioBuffer();
                        inBatch = 0;
                    }
                }

                if (portal.hasNext())
                    writePortalSuspended(buf);
                else
                    writeCommandComplete(buf, portal.tag(), portal.processed());
            } else {
                writeEmptyResponse(buf);
            }
            ctx.write(buf);
        } catch (Exception e) {
            ReferenceCountUtil.release(buf);
            queryProvider.closeP(pName);
            throw e;
        }
    }

    private void onDescribe(ChannelHandlerContext ctx, ByteBuf msg) throws ProtocolException {
        switch (msg.readByte()) {
            case 'S': {
                onDescribeStatement(ctx, readString(msg));
                break;
            }

            case 'P': {
                onDescribePortal(ctx, readString(msg));
                break;
            }

            default:
                throw new BreakingException("08P01" /* protocol violation */, "unexpected describe type");
        }
    }

    private void onDescribePortal(ChannelHandlerContext ctx, String portal) throws ProtocolException {
        RowDescription desc = queryProvider.describeP(portal);
        writeRowDescription(ctx, desc);
    }

    private void onDescribeStatement(ChannelHandlerContext ctx, String stmt) throws ProtocolException {
        StatementDescription desc = queryProvider.describeS(stmt);
        writeParametersDescription(ctx, desc.getParametersDescription());
        writeRowDescription(ctx, desc.getRowDescription());
    }

    private void onBind(ChannelHandlerContext ctx, ByteBuf msg) throws ProtocolException {
        String portal = readString(msg);
        String stmt = readString(msg);
        ParametersDescription desc = queryProvider.describeS(stmt).getParametersDescription();

        int inFcLen = msg.readShort();

        if (inFcLen != desc.getParametersCount() && inFcLen > 1) {
            throw new BreakingException("08P01" /* protocol violation */, "invalid format codes count");
        }

        int[] inFc = new int[inFcLen];
        for (int i = 0; i < inFcLen; i++)
            inFc[i] = msg.readShort();

        int paramsLen = msg.readShort();
        List<ParameterDescription> paramsDesc = desc.getParameters();
        if (paramsLen != paramsDesc.size()) {
            throw new BreakingException("08P01" /* protocol violation */, "invalid parameter count");
        }

        Object[] params = new Object[paramsLen];
        for (int i = 0; i < paramsLen; i++) {
            boolean binary;
            if (inFcLen == 0) {
                binary = false;
            } else if (inFcLen == 1)
                binary = inFc[0] == 1;
            else
                binary = inFc[i] == 1;

            int type = paramsDesc.get(i).getType();
            switch (type) {
                case PG_TYPE_VARCHAR:
                case PG_TYPE_TEXT:
                case PG_TYPE_BPCHAR:
                    params[i] = TypeUtils.readString(msg, type, charset);
                    break;

                case PG_TYPE_BOOL:
                    params[i] = binary ? readBoolB(msg, type) : readBoolS(msg, type, charset);
                    break;

                case PG_TYPE_INT8:
                    params[i] = binary ? readLongB(msg, type) : readLongS(msg, type, charset);
                    break;

                case PG_TYPE_INT2:
                case PG_TYPE_INT4:
                    params[i] = binary ? readIntB(msg, type) : readIntS(msg, type, charset);
                    break;

                default:
                    throw new BreakingException("XX000" /* internal error */, "unsupported data type: " + type);
            }
        }

        int outFcLen = msg.readShort();
        int[] outFc = new int[outFcLen];
        for (int i = 0; i < outFcLen; i++)
            outFc[i] = msg.readShort();

        try {
            queryProvider.bind(portal, stmt, params, outFc);
        } catch (Exception e) {
            throw new NonBreakingException("XX000" /* internal error */, "cannot bind statement", e);
        }

        // BindComplete message
        writeBindComplete(ctx);
    }

    private void onParse(ChannelHandlerContext ctx, ByteBuf msg) throws ProtocolException {
        String stmt = readString(msg);
        String query = readString(msg);
        int paramLen = msg.readShort();
        int[] paramTypes = new int[paramLen];
        for (int i = 0; i < paramLen; i++)
            paramTypes[i] = msg.readInt();
        try {
            queryProvider.prepare(stmt, query, paramTypes);
        } catch (Exception e) {
            throw new NonBreakingException("XX000" /* internal error */, "cannot prepare statement", e);
        }

        // ParseComplete message
        writeParseComplete(ctx);
    }

    private void onPassword(ChannelHandlerContext ctx, ByteBuf msg) throws ProtocolException {
        Authentication auth = authProvider.authenticate(new ClearTextPassword(readString(msg)));
        if (auth != Authentication.OK)
            throw new BreakingException("28P01" /* invalid password */, "Authentication failed");

        onAuthenticationOK(ctx);
    }

    private void onInit(ChannelHandlerContext ctx, ByteBuf msg) throws ProtocolException {
        int version = msg.readInt();
        switch (version) {
            case PROTOCOL_3_0:
                initRead = true;
                Charset newCharset = null;
                while (msg.getByte(msg.readerIndex()) > 0) {
                    String key = readString(msg);
                    String value = readString(msg);

                    switch (key) {
                        case "user": {
                            this.username = value;
                            break;
                        }
                        case "database": {
                            this.database = value;
                            break;
                        }
                        case "client_encoding": {
                            // node-postgres will send "'utf-8'"
                            int length = value.length();
                            if (length >= 2 && value.charAt(0) == '\''
                                    && value.charAt(length - 1) == '\'') {
                                value = value.substring(1, length - 1);
                            }
                            newCharset = Charset.forName(value);
                            break;
                        }
                        case "DateStyle": {
                            dateStyle = value.indexOf(',') < 0 ? value + ", MDY" : value;
                            break;
                        }
                        case "TimeZone": {
                            try {
                                timeZone = TimeZone.getTimeZone(pgTimeZone(value));
                            } catch (Exception e) {
                                log.warn("Unknown TimeZone: " + value, e);
                            }
                            break;
                        }

                        default: {
                            log.trace("param=" + key + "; value=" + value);
                            break;
                        }
                    }
                }

                if (newCharset != null)
                    charset = newCharset;

                // request clear text password
                if (authProvider == AuthenticationProvider.NO_OP_PROVIDER)
                    onAuthenticationOK(ctx);
                else
                    writeRequestClearText(ctx);

                break;
            case CANCEL_REQUEST:
                initRead = true;
                // Cancel request
                queryProvider.cancel(msg.readInt(), msg.readInt());

                ctx.close();

                break;
            case SSL_REQUEST: {
                ctx.write(ctx.alloc().ioBuffer(1).writeByte('N'));

                break;
            }
            default: {
                throw new BreakingException("08P01" /* protocol violation */, "Unsupported driver protocol version: " + (version >> 16) + "." + (version & 0xff));
            }
        }
    }

    private void onAuthenticationOK(ChannelHandlerContext ctx) throws ProtocolException {
        queryProvider.init();

        ByteBuf buf = ctx.alloc().ioBuffer();
        writeAuthenticationOK(buf);
        writeParameterStatus(buf, "client_encoding", charset.name());
        writeParameterStatus(buf, "DateStyle", dateStyle);
        writeParameterStatus(buf, "is_superuser", "off");
        writeParameterStatus(buf, "server_encoding", "SQL_ASCII");
        writeParameterStatus(buf, "server_version", "8.2.23");
        writeParameterStatus(buf, "session_authorization", username);
        writeParameterStatus(buf, "standard_conforming_strings", "off");
        writeParameterStatus(buf, "TimeZone", pgTimeZone(timeZone.getID()));
        writeParameterStatus(buf, "integer_datetimes", "on");
        writeBackendKeyData(buf);
        writeReadyForQuery(buf);
        ctx.write(buf);
    }

    private void writeDataRow(ByteBuf buf, Object row, RowDescription desc) throws ProtocolException {
        Object[] row0; // TODO use right row type
        try {
            row0 = (Object[]) row;
        } catch (Exception e) {
            throw new BreakingException("unexpected row type", e);
        }

        if (row0 == null)
            throw new BreakingException("row is null");
        else if (row0.length != desc.getColumnsCount())
            throw new BreakingException("unexpected row columns count");

        buf.writeByte('D');
        int idx = buf.writerIndex();
        buf.writeInt(0);
        buf.writeShort(desc.getColumnsCount());
        List<ColumnDescription> columns = desc.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            ColumnDescription column = columns.get(i);
            boolean binary = column.getFormatCode() == 1;
            int type = column.getType();
            switch (type) {
                case PG_TYPE_VARCHAR:
                case PG_TYPE_TEXT:
                case PG_TYPE_BPCHAR:
                    writeS(buf, (String) row0[i], type, charset);
                    break;

                case PG_TYPE_BOOL:
                    if (binary)
                        writeB(buf, (Boolean) row0[i], type);
                    else
                        writeS(buf, (Boolean) row0[i], type, charset);

                    break;

                case PG_TYPE_INT8:
                    if (binary)
                        writeB(buf, (Long) row0[i], type);
                    else
                        writeS(buf, (Long) row0[i], type, charset);

                    break;

                case PG_TYPE_INT2:
                case PG_TYPE_INT4:
                    if (binary)
                        writeB(buf, (Integer) row0[i], type);
                    else
                        writeS(buf, (Integer) row0[i], type, charset);

                    break;

                case PG_TYPE_UNKNOWN:
                    if (binary)
                        writeB(buf, row0[i], type);
                    else
                        writeS(buf, row0[i], type, charset);

                    break;

                default:
                    throw new BreakingException("unsupported data type: " + type);
            }
        }

        buf.setInt(idx, buf.writerIndex() - idx);
    }

    private void writeParametersDescription(ChannelHandlerContext ctx, ParametersDescription desc) {
        ByteBuf msg = ctx.alloc().ioBuffer();

        msg.writeByte('t');
        int idx = msg.writerIndex();
        msg.writeInt(0); // reserve a place for msg length

        msg.writeShort(desc.getParametersCount());

        for (ParameterDescription paramDesc : desc.getParameters()) {
            msg.writeInt(paramDesc.getType());
        }

        msg.setInt(idx, msg.writerIndex() - idx);

        ctx.write(msg);
    }

    private void writeRowDescription(ChannelHandlerContext ctx, RowDescription desc) {
        ByteBuf msg = ctx.alloc().ioBuffer();
        writeRowDescription(msg, desc);
        ctx.write(msg);
    }

    private void writeRowDescription(ByteBuf buf, RowDescription desc) {
        if (desc.getColumnsCount() == 0) {
            buf.writeByte('n').writeInt(4);
        } else {
            buf.writeByte('T');
            int idx = buf.writerIndex();
            buf.writeInt(0); // reserve a place for msg length

            buf.writeShort(desc.getColumnsCount());

            for (ColumnDescription columnDesc : desc.getColumns()) {
                writeString(buf, columnDesc.getName());
                buf.writeInt(columnDesc.getTableId());
                buf.writeShort(columnDesc.getTableIndex());
                buf.writeInt(columnDesc.getType());
                buf.writeShort(columnDesc.getTypeLen());
                buf.writeInt(columnDesc.getTypeModifier());
                buf.writeShort(columnDesc.getFormatCode());
            }

            buf.setInt(idx, buf.writerIndex() - idx);
        }
    }

    /**
     * @param buf Destination buffer.
     * @param code Error code, see https://www.postgresql.org/docs/9.3/errcodes-appendix.html for details
     * @param msg Error message
     */
    private void writeError(ByteBuf buf, String code, String msg) {
        buf.writeByte('E');
        int idx = buf.writerIndex();
        buf.writeInt(0); // reserve a place for length
        buf.writeByte('S'); // severity
        writeString(buf, "ERROR");
        buf.writeByte('C'); // code
        writeString(buf, code);
        buf.writeByte('M'); // message
        writeString(buf, msg);

        buf.writeByte(0); // end of fields

        buf.setInt(idx, buf.writerIndex() - idx);

    }

    private void writeParameterStatus(ByteBuf buf, String param, String value) {
        buf.writeByte('S');
        int idx = buf.writerIndex();
        buf.writeInt(0); // reserve a place for length
        writeString(buf, param);
        writeString(buf, value);
        buf.setInt(idx, buf.writerIndex() - idx);
    }

    private void writeCommandComplete(ByteBuf buf, Portal.Tag tag, int processed) {
        buf.writeByte('C');
        int idx = buf.writerIndex();
        buf.writeInt(0);
        writeString(buf, String.format("%s %d", tag, processed));
        buf.setInt(idx, buf.writerIndex() - idx);
    }

    private void writeCloseComplete(ChannelHandlerContext ctx) {
        ctx.write(ctx.alloc().ioBuffer(5).writeByte('3').writeInt(4));
    }

    private void writeBindComplete(ChannelHandlerContext ctx) {
        ctx.write(ctx.alloc().ioBuffer(5).writeByte('2').writeInt(4));
    }

    private void writeParseComplete(ChannelHandlerContext ctx) {
        ctx.write(ctx.alloc().ioBuffer(5).writeByte('1').writeInt(4));
    }

    private void writeRequestClearText(ChannelHandlerContext ctx) {
        ctx.write(ctx.alloc().ioBuffer(9).writeByte('R').writeInt(8).writeInt(3));
    }

    private void writeReadyForQuery(ChannelHandlerContext ctx) {
        ByteBuf buf = ctx.alloc().ioBuffer(6);
        writeReadyForQuery(buf);
        ctx.write(buf);
    }

    private void writeReadyForQuery(ByteBuf buf) {
        buf.writeByte('Z').writeInt(5).writeByte('I');
    }

    private void writeBackendKeyData(ByteBuf buf) {
        buf.writeByte('K').writeInt(12).writeInt(id).writeInt(secret);
    }

    private void writeAuthenticationOK(ByteBuf buf) {
        buf.writeByte('R').writeInt(8).writeInt(0);
    }

    private void writePortalSuspended(ByteBuf buf) {
        buf.writeByte('s').writeInt(4);
    }

    private void writeEmptyResponse(ByteBuf buf) {
        buf.writeByte('I').writeInt(4);
    }

    private String readString(ByteBuf msg) {
        // C string - a null character terminated string
        int len = 0, index = msg.readerIndex();
        while (msg.getByte(index++) != 0) len++; // find end of the string

        String res = msg.readCharSequence(len, charset).toString();
        byte terminator = msg.readByte(); // skip null character
        assert terminator == 0;
        return res;
    }

    private void writeString(ByteBuf msg, String value) {
        msg.writeCharSequence(value, charset);
        msg.writeByte(0); // null character
    }

    private static String pgTimeZone(String value) {
        if (value.startsWith("GMT+")) {
            return convertTimeZone(value, "GMT-");
        } else if (value.startsWith("GMT-")) {
            return convertTimeZone(value, "GMT+");
        } else if (value.startsWith("UTC+")) {
            return convertTimeZone(value, "UTC-");
        } else if (value.startsWith("UTC-")) {
            return convertTimeZone(value, "UTC+");
        } else {
            return value;
        }
    }

    private static String convertTimeZone(String value, String prefix) {
        return prefix + value.substring(4);
    }
}
