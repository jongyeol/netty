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

import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.StringUtil;

/**
 * Simple Strings or Errors of <a href="http://redis.io/topics/protocol">RESP</a>
 */
public class StringRedisMessage implements RedisMessage {

    private final RedisMessageType type;
    private final String content;

    StringRedisMessage(RedisMessageType type, String content) {
        ObjectUtil.checkNotNull(content, "content cannot be null");
        if (type != RedisMessageType.SIMPLE_STRING && type != RedisMessageType.ERROR) {
            throw new IllegalArgumentException("bad type: " + type);
        }
        this.type = type;
        this.content = content;
    }

    public String content() {
        return content;
    }

    @Override
    public RedisMessageType type() {
        return type;
    }

    @Override
    public boolean isNull() {
        return false;
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
