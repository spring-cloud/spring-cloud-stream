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

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.jgroups.BytesMessage;
import org.jgroups.JChannel;
import org.jgroups.Receiver;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.cloud.bus.event.EnvironmentChangeRemoteApplicationEvent;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;
import org.springframework.cloud.bus.jackson.SubtypeModule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JGroupsBusBridgeTests {

	private final JGroupsBusProperties properties = new JGroupsBusProperties();

	private final ObjectMapper objectMapper = JsonMapper.builder()
		.addModule(new SubtypeModule(EnvironmentChangeRemoteApplicationEvent.class))
		.build();

	@Test
	void sendsEventAsBytesMessage() throws Exception {
		JChannel channel = mock(JChannel.class);
		JGroupsChannelFactory channelFactory = mock(JGroupsChannelFactory.class);
		when(channelFactory.create()).thenReturn(channel);

		JGroupsBusBridge bridge = new JGroupsBusBridge(this.properties, this.objectMapper, event -> {
		}, channelFactory);

		RemoteApplicationEvent event = new EnvironmentChangeRemoteApplicationEvent("test", "test", (String) null,
			Map.of());

		bridge.send(event);

		verify(channel).send(any(BytesMessage.class));
	}

	@Test
	void receivesEventAndDelegatesToConsumer() throws Exception {
		JChannel channel = mock(JChannel.class);
		JGroupsChannelFactory channelFactory = mock(JGroupsChannelFactory.class);
		when(channelFactory.create()).thenReturn(channel);

		AtomicReference<RemoteApplicationEvent> received = new AtomicReference<>();

		new JGroupsBusBridge(this.properties, this.objectMapper, received::set, channelFactory);

		ArgumentCaptor<Receiver> receiver = ArgumentCaptor.forClass(Receiver.class);
		verify(channel).setReceiver(receiver.capture());

		EnvironmentChangeRemoteApplicationEvent event = new EnvironmentChangeRemoteApplicationEvent("test", "test",
			(String) null, Map.of());

		byte[] payload = this.objectMapper.writeValueAsBytes(event);
		BytesMessage message = new BytesMessage(null, payload);

		receiver.getValue().receive(message);

		assertThat(received.get()).isNotNull();
		assertThat(received.get().getClass()).isEqualTo(EnvironmentChangeRemoteApplicationEvent.class);
	}

	@Test
	void connectsToConfiguredCluster() throws Exception {
		JChannel channel = mock(JChannel.class);
		JGroupsChannelFactory channelFactory = mock(JGroupsChannelFactory.class);
		when(channelFactory.create()).thenReturn(channel);

		this.properties.setClusterName("test-cluster");

		new JGroupsBusBridge(this.properties, this.objectMapper, event -> {
		}, channelFactory);

		verify(channel).connect("test-cluster");
	}

	@Test
	void closesChannel() throws Exception {
		JChannel channel = mock(JChannel.class);
		JGroupsChannelFactory channelFactory = mock(JGroupsChannelFactory.class);
		when(channelFactory.create()).thenReturn(channel);

		JGroupsBusBridge bridge = new JGroupsBusBridge(this.properties, this.objectMapper, event -> {
		}, channelFactory);

		bridge.close();

		verify(channel).close();
	}

}
