/*
 * Copyright 2015-present the original author or authors.
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

package org.springframework.cloud.bus.jgroups;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import org.jgroups.BytesMessage;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;

import tools.jackson.databind.ObjectMapper;

import org.springframework.cloud.bus.BusBridge;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;

public class JGroupsBusBridge implements BusBridge {

	private final JChannel channel;

	private final ObjectMapper objectMapper;

	public JGroupsBusBridge(JGroupsBusProperties properties, ObjectMapper objectMapper,
			Consumer<RemoteApplicationEvent> eventConsumer, JGroupsChannelFactory channelFactory) {
		try {
			this.objectMapper = objectMapper;
			this.channel = channelFactory.create();
			this.channel.setReceiver(new Receiver() {
				@Override
				public void receive(Message message) {
					try {
						String payload = new String(message.getArray(), message.getOffset(), message.getLength(),
								StandardCharsets.UTF_8);
						RemoteApplicationEvent event = JGroupsBusBridge.this.objectMapper.readValue(payload,
								RemoteApplicationEvent.class);
						eventConsumer.accept(event);
					}
					catch (Exception ex) {
						throw new IllegalStateException("Unable to receive event from JGroups", ex);
					}
				}
			});
			this.channel.connect(properties.getClusterName());
		}
		catch (Exception ex) {
			throw new IllegalStateException("Unable to initialize JGroups channel", ex);
		}
	}

	@Override
	public void send(RemoteApplicationEvent event) {
		try {
			byte[] payload = this.objectMapper.writeValueAsBytes(event);
			this.channel.send(new BytesMessage(null, payload));
		}
		catch (Exception ex) {
			throw new IllegalStateException("Unable to send event over JGroups", ex);
		}
	}

	public void close() {
		this.channel.close();
	}

}
