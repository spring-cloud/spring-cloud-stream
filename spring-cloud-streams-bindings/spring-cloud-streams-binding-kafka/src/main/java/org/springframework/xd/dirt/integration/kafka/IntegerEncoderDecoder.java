/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.xd.dirt.integration.kafka;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import org.springframework.util.Assert;


/**
 * A Kafka encoder / decoder used to serialize a single int, used as the kafka partition key.
 *
 * @author Eric Bottard
 */
public class IntegerEncoderDecoder implements Encoder<Integer>, Decoder<Integer> {


	public IntegerEncoderDecoder() {
		this(new VerifiableProperties());
	}

	public IntegerEncoderDecoder(VerifiableProperties properties) {
	}

	@Override
	public Integer fromBytes(byte[] bytes) {
		Assert.isTrue(bytes.length == 4);
		return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
	}

	@Override
	public byte[] toBytes(Integer message) {
		int value = message.intValue();
		return new byte[] {
				(byte) (value >>> 24),
				(byte) (value >>> 16),
				(byte) (value >>> 8),
				(byte) value
		};
	}

}
