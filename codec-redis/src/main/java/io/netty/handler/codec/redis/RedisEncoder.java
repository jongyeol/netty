/*
 * Copyright 2016 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.netty.handler.codec.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.CharsetUtil;

import java.util.List;

public class RedisEncoder extends MessageToMessageEncoder<RedisObject> {

    private static final int TYPE_LENGTH = 1;
    private static final byte[] CRLF = { '\r', '\n' };
    private static final byte[] NULL_BULK_STRING = { '-', '1' };

    @Override
    protected void encode(ChannelHandlerContext ctx, RedisObject msg, List<Object> out) throws Exception {
        ByteBuf buf = ctx.alloc().buffer(calculateBufferLength(msg));
        out.add(writeRedisMessage(buf, msg));
    }

    private static ByteBuf writeRedisMessage(ByteBuf buf, RedisObject msg) {
        if (msg instanceof StringRedisMessage) {
            writeStringMessage(buf, (StringRedisMessage) msg);
        } else if (msg instanceof IntegerRedisMessage) {
            writeIntegerMessage(buf, (IntegerRedisMessage) msg);
        } else if (msg instanceof BulkStringRedisMessage) {
            writeBulkStringMessage(buf, (BulkStringRedisMessage) msg);
        } else if (msg instanceof ArrayHeaderRedisObject) {
            writeArrayHeaderObject(buf, (ArrayHeaderRedisObject) msg);
        } else if (msg instanceof ArrayRedisMessage) {
            writeArrayMessage(buf, (ArrayRedisMessage) msg);
        } else {
            throw new Error("bad message: " + msg);
        }
        return buf;
    }

    private static void writeStringMessage(ByteBuf buf, StringRedisMessage msg) {
        buf.writeByte(msg.type().value());
        buf.writeBytes(msg.content());
        buf.writeBytes(CRLF);
    }

    private static void writeIntegerMessage(ByteBuf buf, IntegerRedisMessage msg) {
        buf.writeByte(msg.type().value());
        buf.writeBytes(numberToBytes(msg.value()));
        buf.writeBytes(CRLF);
    }

    private static void writeBulkStringMessage(ByteBuf buf, BulkStringRedisMessage msg) {
        buf.writeByte(msg.type().value());
        if (msg.isNull()) {
            buf.writeBytes(NULL_BULK_STRING);
        } else {
            ByteBuf content = msg.content();
            buf.writeBytes(numberToBytes(content.readableBytes()));
            buf.writeBytes(CRLF);
            buf.writeBytes(content);
        }
        buf.writeBytes(CRLF);
    }

    /**
     * Write array header only without body. Use this if you want to write arrays as streaming.
     */
    private static void writeArrayHeaderObject(ByteBuf buf, ArrayHeaderRedisObject msg) {
        buf.writeByte(RedisMessageType.ARRAY.value());
        if (msg.isNull()) {
            buf.writeBytes(NULL_BULK_STRING);
        } else {
            buf.writeBytes(numberToBytes(msg.length()));
        }
        buf.writeBytes(CRLF);
    }

    /**
     * Write full constructed array message.
     */
    private static void writeArrayMessage(ByteBuf buf, ArrayRedisMessage msg) {
        buf.writeByte(msg.type().value());
        if (msg.isNull()) {
            buf.writeBytes(NULL_BULK_STRING);
            buf.writeBytes(CRLF);
        } else {
            buf.writeBytes(numberToBytes(msg.children().size()));
            buf.writeBytes(CRLF);
            for (RedisMessage child : msg.children()) {
                writeRedisMessage(buf, child);
            }
        }
    }

    private static int calculateBufferLength(RedisObject msg) {
        if (msg instanceof StringRedisMessage) {
            return TYPE_LENGTH + ((StringRedisMessage) msg).content().length + CRLF.length;
        } else if (msg instanceof IntegerRedisMessage) {
            return TYPE_LENGTH + getBufferLengthOfNumber(((IntegerRedisMessage) msg).value()) + CRLF.length;
        } else if (msg instanceof BulkStringRedisMessage) {
            final BulkStringRedisMessage message = (BulkStringRedisMessage) msg;
            if (message.isNull()) {
                return TYPE_LENGTH + NULL_BULK_STRING.length + CRLF.length;
            } else {
                final int contentLength = message.content().readableBytes();
                return TYPE_LENGTH + numberToBytes(contentLength).length + CRLF.length
                       + contentLength + CRLF.length;
            }
        } else if (msg instanceof ArrayHeaderRedisObject) {
            return TYPE_LENGTH + getBufferLengthOfNumber(((ArrayHeaderRedisObject) msg).length()) + CRLF.length;
        } else if (msg instanceof ArrayRedisMessage) {
            final ArrayRedisMessage message = (ArrayRedisMessage) msg;
            if (message.children() == null) {
                return TYPE_LENGTH + NULL_BULK_STRING.length + CRLF.length;
            } else {
                int length = TYPE_LENGTH + getBufferLengthOfNumber(message.children().size()) + CRLF.length;
                for (RedisMessage child : message.children()) {
                    length += calculateBufferLength(child);
                }
                return length;
            }
        } else {
            throw new Error("bad message: " + msg);
        }
    }

    private static byte[] numberToBytes(long value) {
        return Long.toString(value).getBytes(CharsetUtil.US_ASCII);
    }

    private static int getBufferLengthOfNumber(long value) {
        return numberToBytes(value).length;
    }
}
