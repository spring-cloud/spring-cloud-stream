/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.config.aggregate.source;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.core.MessageSource;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

/**
 * @author Ilayaperumal Gopinathan
 */
@EnableBinding(Source.class)
@EnableAutoConfiguration
@Configuration
public class TestSource {

	@Bean
	@InboundChannelAdapter(Source.OUTPUT)
	public MessageSource<String> timerMessageSource() {
		return new MessageSource<String>() {
			@Override
			public Message<String> receive() {
				return MessageBuilder
						.withPayload(new SimpleDateFormat("DDMMMYYYY").format(new Date()))
						.setHeader(MessageHeaders.CONTENT_TYPE, "text/plain").build();
			}
		};
	}

}
