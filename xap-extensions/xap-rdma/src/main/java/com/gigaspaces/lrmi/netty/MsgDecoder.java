package com.gigaspaces.lrmi.netty;

import com.gigaspaces.lrmi.rdma.RdmaMsg;
import com.ibm.disni.util.DiSNILogger;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.function.Function;

public class MsgDecoder extends LengthFieldBasedFrameDecoder {
    private static final Logger logger = Logger.getLogger(MsgDecoder.class);
    private Function<ByteBuf, Object> deserialize;

    public MsgDecoder(Function<ByteBuf, Object> deserialize) {
        super(1048576, 0, 4, 0, 4);
        this.deserialize = deserialize;
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if (frame == null) {
            return null;
        }
        long id = frame.readLong();

        try {
            Object payload = deserialize.apply(frame);
            RdmaMsg msg = new RdmaMsg(payload);
            msg.setId(id);
            return msg;
        } catch (Exception e) {
            logger.error(e, e);
//            throw e;
            return e;
        }
    }

    @Override
    protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
        return buffer.slice(index, length);
    }

}
