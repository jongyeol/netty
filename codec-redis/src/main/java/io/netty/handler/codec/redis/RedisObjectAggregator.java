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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

public class RedisObjectAggregator extends MessageToMessageDecoder<RedisObject> {

    private final Deque<AggregateState> depths;

    public RedisObjectAggregator() {
        depths = new ArrayDeque<AggregateState>();
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, RedisObject msg, List<Object> out) throws Exception {
        RedisMessage message;

        if (msg instanceof RedisMessage) {
            message = (RedisMessage) msg;
            ReferenceCountUtil.retain(message);
        } else if (msg instanceof ArrayHeaderRedisObject) {
            message = decodeRedisArrayHeader((ArrayHeaderRedisObject) msg);
        } else {
            throw new Error();
        }

        if (message != null) {
            while (!depths.isEmpty()) {
                AggregateState current = depths.peek();
                current.children.add(message);

                // if current aggregation completed, go to parent aggregation.
                if (current.children.size() == current.length) {
                    message = new DefaultArrayRedisMessage(current.children);
                    depths.pop();
                } else {
                    // not aggregated yet. try next time.
                    return;
                }
            }

            out.add(message);
        }
    }

    private RedisMessage decodeRedisArrayHeader(ArrayHeaderRedisObject header) {
        if (header.isNull()) {
            return NullArrayRedisMessage.INSTANCE;
        } else if (header.length() == 0L) {
            return EmptyArrayRedisMessage.INSTANCE;
        } else if (header.length() > 0L) {
            // Currently, this codec doesn't support `long` length for arrays because Java's List.size() is int.
            if (header.length() > Integer.MAX_VALUE) {
                throw new IllegalStateException("this codec doesn't support longer length than " + Integer.MAX_VALUE);
            }

            // start aggregating array
            depths.push(new AggregateState((int) header.length()));
            return null;
        } else {
            throw new IllegalArgumentException("bad length: " + header.length());
        }
    }

    private static class AggregateState {
        int length;
        List<RedisMessage> children;
        AggregateState(int length) {
            this.length = length;
            this.children = new ArrayList<RedisMessage>(length);
        }
    }
}
