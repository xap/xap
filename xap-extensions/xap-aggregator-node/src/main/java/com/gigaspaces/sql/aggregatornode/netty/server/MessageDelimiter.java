package com.gigaspaces.sql.aggregatornode.netty.server;

import com.gigaspaces.sql.aggregatornode.netty.utils.Constants;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class MessageDelimiter extends ByteToMessageDecoder {
    private boolean initRead;

    private ByteBuf incomplete;
    private int remaining;

    /**
     * All messages has the following layout:
     *     +------+--------+------+
     *    | type | length | body |
     *    +------+--------+------+
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        while (true) {
            if (incomplete != null) {
                int toRead = Math.min(remaining, in.readableBytes());
                in.readBytes(incomplete, toRead);
                if ((remaining -= toRead) > 0)
                    break; // no more data to read

                out.add(incomplete);
                incomplete = null;

                continue;
            }

            if (!initRead && !in.isReadable(8))
                break;

            if (!in.isReadable(headerSize()))
                break;

            boolean isInit = !initRead && in.getInt(in.readerIndex() + 4) != Constants.SSL_REQUEST;

            int size = messageSize(in);
            if (in.isReadable(size))
                out.add(in.readRetainedSlice(size));
            else
                incomplete = ctx.alloc().heapBuffer(remaining = size);

            initRead |= isInit;
        }
    }

    private int headerSize() {
        return initRead ? 5 : 4;
    }

    private int messageSize(ByteBuf in) {
        assert remaining == 0;
        assert incomplete == null;
        assert in.isReadable(headerSize());

        int shift = initRead ? 1 : 0;
        return in.getInt(in.readerIndex() + shift) + shift;
    }
}
