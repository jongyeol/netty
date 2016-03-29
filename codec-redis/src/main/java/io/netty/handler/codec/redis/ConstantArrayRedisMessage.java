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

import java.util.List;

public final class ConstantArrayRedisMessage implements ArrayRedisMessage {

    private List<RedisMessage> children;

    ConstantArrayRedisMessage(List<RedisMessage> children) {
        this.children = children;
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
    public List<RedisMessage> children() {
        return children;
    }
}
