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
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.internal.StringUtil;

/**
 * Bulk Strings of <a href="http://redis.io/topics/protocol">RESP</a>
 */
public class BulkStringRedisMessage extends AbstractReferenceCounted implements RedisMessage {

    public static final BulkStringRedisMessage NULL_BULK_STRING = new BulkStringRedisMessage(null);
    public static final BulkStringRedisMessage EMPTY_BULK_STRING = new BulkStringRedisMessage(Unpooled.EMPTY_BUFFER);

    private ByteBuf content;

    public BulkStringRedisMessage(ByteBuf content) {
        this.content = content;
        if (content != null) {
            this.content.retain(); // for hold bytebuf
        }
    }

    @Override
    public RedisMessageType type() {
        return RedisMessageType.BULK_STRING;
    }

    public ByteBuf content() {
        return content;
    }

    @Override
    public boolean isNull() {
        return content == null;
    }

    @Override
    protected void deallocate() {
        if (content != null) {
            content.release();
        }
    }

    @Override
    public BulkStringRedisMessage touch(Object hint) {
        if (content != null) {
            content.touch();
        }
        return this;
    }

    @Override
    public String toString() {
        return new StringBuilder(StringUtil.simpleClassName(this))
                .append('[')
                .append("content=")
                .append(content)
                .append(']').toString();
    }
}
