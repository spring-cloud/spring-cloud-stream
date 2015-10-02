/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package demo;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.converter.AbstractFromMessageConverter;
import org.springframework.cloud.stream.converter.MessageConverterUtils;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.core.MessageSource;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

/**
 * @author Dave Syer
 * @author Glenn Renfro
 *
 */
@EnableBinding(Source.class)
@EnableConfigurationProperties(TimeSourceOptionsMetadata.class)
public class TimeSource {

	@Autowired
	private TimeSourceOptionsMetadata options;

	@Bean
	@InboundChannelAdapter(value = Source.OUTPUT, poller = @Poller(fixedDelay = "${fixedDelay}", maxMessagesPerPoll = "1"))
	public MessageSource<String> timerMessageSource() {
		return () -> new GenericMessage<>(new SimpleDateFormat(this.options.getFormat()).format(new Date()));
	}

	private final static List<MimeType> targetMimeTypes = new ArrayList<MimeType>();
	static {
		targetMimeTypes.add(MessageConverterUtils.X_JAVA_OBJECT);
		targetMimeTypes.add(MimeTypeUtils.TEXT_PLAIN);
	}

	@Bean
	public List<AbstractFromMessageConverter> customMessageConverters() {
		List<AbstractFromMessageConverter> converters = new ArrayList<>();
		converters.add(new AbstractFromMessageConverter(targetMimeTypes) {
			@Override
			protected Class<?>[] supportedTargetTypes() {
				return new Class<?>[] { String.class };
			}

			@Override
			protected Class<?>[] supportedPayloadTypes() {
				return new Class<?>[] { byte[].class };
			}


		});
		return converters;
	}
}
