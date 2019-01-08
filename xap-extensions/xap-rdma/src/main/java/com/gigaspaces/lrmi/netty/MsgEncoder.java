package com.gigaspaces.lrmi.netty;

import com.gigaspaces.lrmi.rdma.RdmaMsg;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.function.BiFunction;

public class MsgEncoder extends MessageToByteEncoder<RdmaMsg> {
    private BiFunction<Object, ByteBuf, IOException> serialize;

    public MsgEncoder(BiFunction<Object, ByteBuf, IOException> serialize) {
        this.serialize = serialize;
    }

    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

    @Override
    protected void encode(ChannelHandlerContext ctx, RdmaMsg msg, ByteBuf out) throws IOException {
        int startIdx = out.writerIndex();
        out.writeBytes(LENGTH_PLACEHOLDER);
        out.writeLong(msg.getId());
        IOException e = serialize.apply(msg.getRequest(), out);
        if (e != null){
            throw e;
        }
        int endIdx = out.writerIndex();
        out.setInt(startIdx, endIdx - startIdx - 4); // write the len
        ctx.flush();
    }
}
