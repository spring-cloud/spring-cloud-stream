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

package source;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.streams.annotation.EnableModule;
import org.springframework.cloud.streams.annotation.Output;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.SourcePollingChannelAdapterSpec;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.dsl.support.Consumer;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

/**
 * @author Dave Syer
 *
 */
@EnableModule
@EnableConfigurationProperties(TimeSourceOptionsMetadata.class)
@EnableIntegration
public class TimeSource {

	@Autowired
	private TimeSourceOptionsMetadata options;

	@Output
	public MessageChannel output;

	// Return a MessageSource

//	@Bean
//	@InboundChannelAdapter(value = "output", autoStartup = "false", poller = @Poller(fixedDelay = "${fixedDelay}", maxMessagesPerPoll = "1"))
//	public MessageSource<String> timerMessageSource() {
//		return () -> new GenericMessage<>(new SimpleDateFormat(this.options.getFormat()).format(new Date()));
//	}

	//Since @EnableModule is now also a message endpoint - can return just the string

//	@InboundChannelAdapter(value = "output", autoStartup = "false", poller = @Poller(fixedDelay = "${fixedDelay}", maxMessagesPerPoll = "1"))
//	public String time() {
//		return new SimpleDateFormat(this.options.getFormat()).format(new Date());
//	}

	// Using SI Java DSL - broken out into two helper methods for clarity
	public MessageSource<String> messageSource() {
		return () -> new GenericMessage<>(new SimpleDateFormat(this.options.getFormat()).format(new Date()));
	}

	public PollerMetadata pollerMetadata() {
		return Pollers.fixedDelay(options.getFixedDelay(),
				TimeUnit.valueOf(options.getTimeUnit())).maxMessagesPerPoll(1).get();
	}

	@Bean
	public IntegrationFlow flow() {
		return IntegrationFlows.from(messageSource(),
				adapterSpec -> adapterSpec.poller(pollerMetadata()).autoStartup(false)).channel(output).get();
	}

//  Tried to inline the MessageSource, but required a cast.
//	@Bean
//	public IntegrationFlow flow2() {
//		return IntegrationFlows.from((MessageSource<?>) new GenericMessage<>(new SimpleDateFormat(this.options.getFormat()).format(new Date())),
//				adapterSpec -> adapterSpec.poller(pollerMetadata()).autoStartup(false)).channel(output).get();
//	}


}