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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.util.Collections;
import java.util.List;

public final class RedisMessageFactory {

    private static final PredefinedString[] PREDEFINED_SIMPLE_STRINGS = {
            new PredefinedString(RedisMessageType.SIMPLE_STRING, "OK"),
            new PredefinedString(RedisMessageType.SIMPLE_STRING, "QUEUED")
            // TODO add predefined simple strings for Redis responses
    };

    private static final PredefinedString[] PREDEFINED_ERRORS = {
            new PredefinedString(RedisMessageType.ERROR, "ERR")
            // TODO add predefined errors for Redis responses
    };

    private static final ConstantBulkStringRedisMessage NULL_BULK_STRING = new ConstantBulkStringRedisMessage(null);

    private static final ConstantBulkStringRedisMessage EMPTY_BULK_STRING =
                                                new ConstantBulkStringRedisMessage(Unpooled.EMPTY_BUFFER);

    private static final ConstantArrayRedisMessage NULL_ARRAY = new ConstantArrayRedisMessage(null);

    private static final ConstantArrayRedisMessage EMPTY_ARRAY =
                                                new ConstantArrayRedisMessage(Collections.<RedisMessage>emptyList());

    private RedisMessageFactory() {
    }

    public static RedisMessage createSimpleString(String content) {
        RedisMessage msg = findPredefinedString(PREDEFINED_SIMPLE_STRINGS, content);
        return msg != null ? msg : new StringRedisMessage(RedisMessageType.SIMPLE_STRING, content);
    }

    public static RedisMessage createSimpleString(ByteBuf content) {
        RedisMessage msg = findPredefinedString(PREDEFINED_SIMPLE_STRINGS, content);
        return msg != null ? msg : new StringRedisMessage(RedisMessageType.SIMPLE_STRING,
                                                          content.toString(CharsetUtil.UTF_8));
    }

    public static RedisMessage createError(String content) {
        RedisMessage msg = findPredefinedString(PREDEFINED_ERRORS, content);
        return msg != null ? msg : new StringRedisMessage(RedisMessageType.ERROR, content);
    }

    public static RedisMessage createError(ByteBuf content) {
        RedisMessage msg = findPredefinedString(PREDEFINED_ERRORS, content);
        return msg != null ? msg : new StringRedisMessage(RedisMessageType.ERROR, content.toString(CharsetUtil.UTF_8));
    }

    public static RedisMessage createInteger(long value) {
        return new IntegerRedisMessage(value);
    }

    public static RedisMessage nullBulkString() {
        return NULL_BULK_STRING;
    }

    public static RedisMessage emptyBulkString() {
        return EMPTY_BULK_STRING;
    }

    public static RedisMessage createBulkString(ByteBuf byteBuf) {
        if (byteBuf == null) {
            return NULL_BULK_STRING;
        } else if (!byteBuf.isReadable()) {
            return EMPTY_BULK_STRING;
        }
        return new RefCountedBulkStringRedisMessage(byteBuf);
    }

    public static RedisMessage createArrayHeader(long length) {
        return new ArrayHeaderRedisMessage(length);
    }

    public static RedisMessage nullArray() {
        return NULL_ARRAY;
    }

    public static RedisMessage emptyArray() {
        return EMPTY_ARRAY;
    }

    public static RedisMessage createArray(List<RedisMessage> children) {
        if (children == null) {
            return NULL_ARRAY;
        } else if (children.isEmpty()) {
            return EMPTY_ARRAY;
        } else {
            return new RefCountedArrayRedisMessage(children);
        }
    }

    private static RedisMessage findPredefinedString(PredefinedString[] predefinedStrings, ByteBuf byteBuf) {
        for (PredefinedString predefined : predefinedStrings) {
            if (ByteBufUtil.equals(predefined.byteBuf, byteBuf)) {
                return predefined.message;
            }
        }
        return null;
    }

    private static RedisMessage findPredefinedString(PredefinedString[] predefinedStrings, String content) {
        for (PredefinedString predefined : predefinedStrings) {
            if (predefined.content.equals(content)) {
                return predefined.message;
            }
        }
        return null;
    }

    private static class PredefinedString {
        private String content;
        private ByteBuf byteBuf;
        private RedisMessage message;
        PredefinedString(RedisMessageType type, String content) {
            this.content = content;
            this.byteBuf = Unpooled.wrappedBuffer(content.getBytes(CharsetUtil.UTF_8));
            this.message = new StringRedisMessage(type, content);
        }
    }
}
