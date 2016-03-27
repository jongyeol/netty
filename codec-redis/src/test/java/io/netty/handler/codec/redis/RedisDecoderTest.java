/*
 * Copyright 2016 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.handler.codec.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import static io.netty.handler.codec.redis.RedisCodecTestUtil.*;

/**
 * Verifies the correct functionality of the {@link RedisObjectDecoder} and {@link RedisObjectAggregator}.
 */
public class RedisDecoderTest {

    private EmbeddedChannel channel;

    @Before
    public void setup() throws Exception {
        channel = new EmbeddedChannel(new RedisObjectDecoder(), new RedisObjectAggregator());
    }

    @After
    public void teardown() throws Exception {
        channel.finish();
    }

    @Test
    public void shouldDecodeSimpleString() {
        byte[] content = bytesOf("OK");
        channel.writeInbound(byteBufOf("+"));
        channel.writeInbound(byteBufOf(content));
        channel.writeInbound(byteBufOf("\r\n"));

        SimpleStringRedisMessage msg = channel.readInbound();

        assertThat(msg.type(), is(RedisMessageType.SIMPLE_STRING));
        assertThat(msg.content(), is(content));
    }

    @Test
    public void shouldDecodeError() {
        byte[] content = bytesOf("ERROR sample message");
        channel.writeInbound(byteBufOf("-"));
        channel.writeInbound(byteBufOf(content));
        channel.writeInbound(byteBufOf("\r"));
        channel.writeInbound(byteBufOf("\n"));

        ErrorRedisMessage msg = channel.readInbound();

        assertThat(msg.type(), is(RedisMessageType.ERROR));
        assertThat(msg.content(), is(content));
    }

    @Test
    public void shouldDecodeInteger() {
        long value = 1234L;
        byte[] content = bytesOf(value);
        channel.writeInbound(byteBufOf(":"));
        channel.writeInbound(byteBufOf(content));
        channel.writeInbound(byteBufOf("\r\n"));

        IntegerRedisMessage msg = channel.readInbound();

        assertThat(msg.type(), is(RedisMessageType.INTEGER));
        assertThat(msg.value(), is(value));
    }

    @Test
    public void shouldDecodeBulkString() {
        String buf1 = "bulk\nst";
        String buf2 = "ring\ntest\n1234";
        byte[] content = bytesOf(buf1 + buf2);
        channel.writeInbound(byteBufOf("$"));
        channel.writeInbound(byteBufOf(Integer.toString(content.length)));
        channel.writeInbound(byteBufOf("\r\n"));
        channel.writeInbound(byteBufOf(buf1));
        channel.writeInbound(byteBufOf(buf2));
        channel.writeInbound(byteBufOf("\r\n"));

        BulkStringRedisMessage msg = channel.readInbound();

        assertThat(msg.type(), is(RedisMessageType.BULK_STRING));
        assertThat(bytesOf(msg.content()), is(content));

        msg.release();
    }

    @Test
    public void shouldDecodeEmptyBulkString() {
        byte[] content = bytesOf("");
        channel.writeInbound(byteBufOf("$"));
        channel.writeInbound(byteBufOf(Integer.toString(content.length)));
        channel.writeInbound(byteBufOf("\r\n"));
        channel.writeInbound(byteBufOf(content));
        channel.writeInbound(byteBufOf("\r\n"));

        BulkStringRedisMessage msg = channel.readInbound();

        assertThat(msg.type(), is(RedisMessageType.BULK_STRING));
        assertThat(bytesOf(msg.content()), is(content));

        msg.release();
    }

    @Test
    public void shouldDecodeNullBulkString() {
        channel.writeInbound(byteBufOf("$"));
        channel.writeInbound(byteBufOf(Integer.toString(-1)));
        channel.writeInbound(byteBufOf("\r\n"));

        BulkStringRedisMessage msg = channel.readInbound();

        assertThat(msg.type(), is(RedisMessageType.BULK_STRING));
        assertThat(msg.content(), is(nullValue()));

        msg.release();
    }

    @Test
    public void shouldDecodeSimpleArray() throws Exception {
        channel.writeInbound(byteBufOf("*3\r\n"));
        channel.writeInbound(byteBufOf(":1234\r\n"));
        channel.writeInbound(byteBufOf("+sim"));
        channel.writeInbound(byteBufOf("ple\r\n-err"));
        channel.writeInbound(byteBufOf("or\r\n"));

        ArrayRedisMessage msg = channel.readInbound();
        List<RedisMessage> children = msg.children();

        assertThat(msg.type(), is(RedisMessageType.ARRAY));
        assertThat(msg.children().size(), is(equalTo(3)));

        assertThat(children.get(0).type(), is(RedisMessageType.INTEGER));
        assertThat(((IntegerRedisMessage) children.get(0)).value(), is(1234L));
        assertThat(children.get(1).type(), is(RedisMessageType.SIMPLE_STRING));
        assertThat(((SimpleStringRedisMessage) children.get(1)).content(), is(bytesOf("simple")));
        assertThat(children.get(2).type(), is(RedisMessageType.ERROR));
        assertThat(((ErrorRedisMessage) children.get(2)).content(), is(bytesOf("error")));

        msg.release();
    }

    @Test
    public void shouldDecodeNestedArray() throws Exception {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(byteBufOf("*2\r\n"));
        buf.writeBytes(byteBufOf("*3\r\n:1\r\n:2\r\n:3\r\n"));
        buf.writeBytes(byteBufOf("*2\r\n+Foo\r\n-Bar\r\n"));
        channel.writeInbound(buf);

        ArrayRedisMessage msg = channel.readInbound();
        List<RedisMessage> children = msg.children();

        assertThat(msg.type(), is(RedisMessageType.ARRAY));
        assertThat(msg.children().size(), is(2));

        ArrayRedisMessage intArray = (ArrayRedisMessage) children.get(0);
        ArrayRedisMessage strArray = (ArrayRedisMessage) children.get(1);

        assertThat(intArray.type(), is(RedisMessageType.ARRAY));
        assertThat(intArray.children().size(), is(3));
        assertThat(((IntegerRedisMessage) intArray.children().get(0)).value(), is(1L));
        assertThat(((IntegerRedisMessage) intArray.children().get(1)).value(), is(2L));
        assertThat(((IntegerRedisMessage) intArray.children().get(2)).value(), is(3L));

        assertThat(strArray.type(), is(RedisMessageType.ARRAY));
        assertThat(strArray.children().size(), is(2));
        assertThat(((SimpleStringRedisMessage) strArray.children().get(0)).content(), is(bytesOf("Foo")));
        assertThat(((ErrorRedisMessage) strArray.children().get(1)).content(), is(bytesOf("Bar")));

        msg.release();
    }

    @Test(expected = IllegalReferenceCountException.class)
    public void shouldErrorOnDoubleReleaseArrayReferenceCounted() throws Exception {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(byteBufOf("*2\r\n"));
        buf.writeBytes(byteBufOf("*3\r\n:1\r\n:2\r\n:3\r\n"));
        buf.writeBytes(byteBufOf("*2\r\n+Foo\r\n-Bar\r\n"));
        channel.writeInbound(buf);

        ArrayRedisMessage msg = channel.readInbound();

        msg.release();
        msg.release();
    }

    @Test(expected = IllegalReferenceCountException.class)
    public void shouldErrorOnReleaseArrayChildReferenceCounted() throws Exception {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(byteBufOf("*2\r\n"));
        buf.writeBytes(byteBufOf("*3\r\n:1\r\n:2\r\n:3\r\n"));
        buf.writeBytes(byteBufOf("$3\r\nFoo\r\n"));
        channel.writeInbound(buf);

        ArrayRedisMessage msg = channel.readInbound();

        List<RedisMessage> children = msg.children();
        msg.release();
        ReferenceCountUtil.release(children.get(1));
    }

    @Test(expected = IllegalReferenceCountException.class)
    public void shouldErrorOnReleasecontentOfArrayChildReferenceCounted() throws Exception {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(byteBufOf("*2\r\n"));
        buf.writeBytes(byteBufOf("$3\r\nFoo\r\n$3\r\nBar\r\n"));
        channel.writeInbound(buf);

        ArrayRedisMessage msg = channel.readInbound();

        List<RedisMessage> children = msg.children();
        ByteBuf childBuf = ((BulkStringRedisMessage) children.get(0)).content();
        msg.release();
        childBuf.release();
    }

}
