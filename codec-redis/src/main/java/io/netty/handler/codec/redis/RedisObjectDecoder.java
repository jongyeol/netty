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
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.ByteProcessor;

import java.util.List;

public class RedisObjectDecoder extends ByteToMessageDecoder {

    private static final int CRLF_LENGTH = 2;
    private static final long NULL_RESPONSE = -1;

    private final ToLongProcessor toLongProcessor = new ToLongProcessor();

    // current decoding states
    private State state;
    private RedisMessageType type;

    // for decoding bulk string
    private long bulkStringLength;

    private enum State {
        DECODE_TYPE,
        DECODE_INLINE, // SIMPLE_STRING, ERROR, INTEGER
        DECODE_LENGTH, // BULK_STRING, ARRAY
        DECODE_BULK_STRING,
    }

    public RedisObjectDecoder() {
        resetDecoder();
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try {
            boolean next;
            do {
                switch (state) {
                case DECODE_TYPE:
                    next = decodeType(in);
                    break;
                case DECODE_INLINE:
                    next = decodeInline(in, out);
                    break;
                case DECODE_LENGTH:
                    next = decodeLength(in, out);
                    break;
                case DECODE_BULK_STRING:
                    next = decodeBulkString(in, out);
                    break;
                default:
                    throw new Error("Unknown state: " + state);
                }
            } while (next);
        } catch (Exception e) {
            resetDecoder();
            throw e;
        }
    }

    private void resetDecoder() {
        state = State.DECODE_TYPE;
    }

    private boolean decodeType(ByteBuf in) throws Exception {
        if (in.readableBytes() < 1) {
            return false;
        }
        type = RedisMessageType.valueOf(in.readByte());
        state = type.isInline() ? State.DECODE_INLINE : State.DECODE_LENGTH;
        return true;
    }

    private boolean decodeInline(ByteBuf in, List<Object> out) throws Exception {
        ByteBuf lineBytes = readLine(in);
        if (lineBytes == null) {
            return false;
        }
        fireMessage(out, newInlineRedisMessage(type, lineBytes));
        return true;
    }

    private boolean decodeLength(ByteBuf in, List<Object> out) throws Exception {
        ByteBuf lineByteBuf = readLine(in);
        if (lineByteBuf == null) {
            return false;
        }
        final long length = parseRedisNumber(lineByteBuf);
        if (type == RedisMessageType.ARRAY) {
            fireMessage(out, new ArrayHeaderRedisObject(length));
            return true;
        } else if (type == RedisMessageType.BULK_STRING) {
            bulkStringLength = length;
            state = State.DECODE_BULK_STRING;
            return true;
        } else {
            throw new Error("bad type: " + type);
        }
    }

    private boolean decodeBulkString(ByteBuf in, List<Object> out) throws Exception {
        if (bulkStringLength == NULL_RESPONSE) {
            // $-1\r\n
            fireMessage(out, NullBulkStringRedisMessage.INSTANCE);
        } else if (bulkStringLength == 0L) {
            // $0\r\n <here> \r\n
            if (in.readableBytes() < CRLF_LENGTH) {
                return false;
            }
            in.skipBytes(CRLF_LENGTH);
            fireMessage(out, EmptyBulkStringRedisMessage.INSTANCE);
        } else if (bulkStringLength > 0L) {
            // ${bulkStringLength}\r\n <here> {data...}\r\n
            if (in.readableBytes() < bulkStringLength + CRLF_LENGTH) {
                return false;
            }
            ByteBuf content = in.readSlice((int) bulkStringLength);
            in.skipBytes(CRLF_LENGTH);
            fireMessage(out, new DefaultBulkStringRedisMessage(content));
        } else {
            throw new IllegalArgumentException("bad bulkStringLength: " + bulkStringLength);
        }
        return true;
    }

    private void fireMessage(List<Object> out, RedisObject msg) {
        out.add(msg);
        resetDecoder();
    }

    private RedisMessage newInlineRedisMessage(RedisMessageType messageType, ByteBuf bytes) {
        switch (messageType) {
        case SIMPLE_STRING:
            return new SimpleStringRedisMessage(bytes);
        case ERROR:
            return new ErrorRedisMessage(bytes);
        case INTEGER:
            return new IntegerRedisMessage(parseRedisNumber(bytes));
        default:
            throw new IllegalArgumentException("bad type: " + type);
        }
    }

    private static ByteBuf readLine(ByteBuf in) {
        final int lfIndex = in.forEachByte(ByteProcessor.FIND_LF);
        if (lfIndex < 0) {
            return null;
        }
        final int length = lfIndex - in.readerIndex() - 1; // `-1` is for CR
        return readBytes(in, length);
    }

    private static ByteBuf readBytes(ByteBuf in, int length) {
        ByteBuf data = in.readSlice(length);
        in.skipBytes(CRLF_LENGTH); // skip CRLF
        return data;
    }

    public long parseRedisNumber(ByteBuf byteBuf) {
        toLongProcessor.result = 0;
        toLongProcessor.first = true;
        byteBuf.forEachByte(toLongProcessor);
        if (!toLongProcessor.negative) {
            return -toLongProcessor.result;
        }
        return toLongProcessor.result;
    }

    private static class ToLongProcessor implements ByteProcessor {
        private long result;
        private boolean negative;
        private boolean first;

        @Override
        public boolean process(byte value) throws Exception {
            if (first) {
                first = false;

                if (value == '-') {
                    negative = true;
                } else {
                    negative = false;
                    int digit = value - '0';
                    result = result * 10 - digit;
                }
                return true;
            }

            int digit = value - '0';
            result = result * 10 - digit;

            return true;
        }

        public long content() {
            if (!negative) {
                return -result;
            }
            return result;
        }

        public void reset() {
            first = true;
            result = 0;
        }
    }
}
