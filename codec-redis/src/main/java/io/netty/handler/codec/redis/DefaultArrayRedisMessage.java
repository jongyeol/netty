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

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.StringUtil;

import java.util.List;

public class DefaultArrayRedisMessage extends AbstractReferenceCounted implements ArrayRedisMessage {

    private final List<RedisMessage> children;

    public DefaultArrayRedisMessage(List<RedisMessage> children) {
        ObjectUtil.checkNotNull(children, "children cannot be null");
        this.children = children; // do not retain here. children are already retained when created.
    }

    @Override
    public List<RedisMessage> children() {
        return children;
    }

    @Override
    public RedisMessageType type() {
        return RedisMessageType.ARRAY;
    }

    @Override
    public boolean isNull() {
        return children == null;
    }

    @Override
    protected void deallocate() {
        for (RedisMessage msg : children) {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public DefaultArrayRedisMessage touch(Object hint) {
        return this;
    }

    @Override
    public String toString() {
        return new StringBuilder(StringUtil.simpleClassName(this))
                .append('[')
                .append("children=")
                .append(children != null ? children.size() : null)
                .append(']').toString();
    }
}
