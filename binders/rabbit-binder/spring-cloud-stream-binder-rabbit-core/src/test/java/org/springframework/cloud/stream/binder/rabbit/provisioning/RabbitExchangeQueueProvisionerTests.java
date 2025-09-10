/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.provisioning;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.impl.AMQImpl.Queue.DeclareOk;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties.AlternateExchange;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties.AlternateExchange.Binding;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.ApplicationContext;
import org.springframework.expression.common.LiteralExpression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @since 3.2.3
 *
 */
class RabbitExchangeQueueProvisionerTests {

	@Test
	void consumerDeclarationsWithDlq() throws IOException {
		ConnectionFactory cf = mock(ConnectionFactory.class);
		Connection conn = mock(Connection.class);
		given(cf.createConnection()).willReturn(conn);
		Channel channel = mock(Channel.class);
		willReturn(new DeclareOk("x", 0, 0))
				.given(channel).queueDeclare(any(), eq(Boolean.TRUE), eq(Boolean.FALSE), eq(Boolean.FALSE), any());
		given(conn.createChannel(anyBoolean())).willReturn(channel);
		RabbitExchangeQueueProvisioner provisioner = new RabbitExchangeQueueProvisioner(cf);

		RabbitConsumerProperties props = new RabbitConsumerProperties();
		props.setAutoBindDlq(true);
		ExtendedConsumerProperties<RabbitConsumerProperties> properties =
				new ExtendedConsumerProperties<RabbitConsumerProperties>(props);
		ConsumerDestination dest = provisioner.provisionConsumerDestination("foo", "group", properties);
		ApplicationContext ctx =
				TestUtils.getPropertyValue(provisioner, "autoDeclareContext", ApplicationContext.class);
		Set<String> declarables = ctx.getBeansOfType(Declarables.class).keySet();
		assertThat(declarables).contains("foo.group.exchange", "foo.group", "foo.group.binding", "foo.group.dlq",
				"DLX.group.exchange", "foo.group.dlq.binding", "foo.group.dlq.2.binding");
		provisioner.cleanAutoDeclareContext(dest, properties);
		assertThat(ctx.getBeansOfType(Declarables.class)).isEmpty();
	}

	@Test
	void consumerDeclarationsWithDlqQueueNameIsGroup() throws IOException {
		ConnectionFactory cf = mock(ConnectionFactory.class);
		Connection conn = mock(Connection.class);
		given(cf.createConnection()).willReturn(conn);
		Channel channel = mock(Channel.class);
		willReturn(new DeclareOk("x", 0, 0))
				.given(channel).queueDeclare(any(), eq(Boolean.TRUE), eq(Boolean.FALSE), eq(Boolean.FALSE), any());
		given(conn.createChannel(anyBoolean())).willReturn(channel);
		RabbitExchangeQueueProvisioner provisioner = new RabbitExchangeQueueProvisioner(cf);

		RabbitConsumerProperties props = new RabbitConsumerProperties();
		props.setAutoBindDlq(true);
		props.setQueueNameGroupOnly(true);
		ExtendedConsumerProperties<RabbitConsumerProperties> properties =
				new ExtendedConsumerProperties<RabbitConsumerProperties>(props);
		ConsumerDestination dest = provisioner.provisionConsumerDestination("fiz", "group", properties);
		ApplicationContext ctx =
				TestUtils.getPropertyValue(provisioner, "autoDeclareContext", ApplicationContext.class);
		Set<String> declarables = ctx.getBeansOfType(Declarables.class).keySet();
		assertThat(declarables).contains("fiz.group.exchange", "group", "group.binding", "group.dlq",
				"DLX.group.exchange", "group.dlq.binding", "group.dlq.2.binding");
		provisioner.cleanAutoDeclareContext(dest, properties);
		assertThat(ctx.getBeansOfType(Declarables.class)).isEmpty();
	}

	@Test
	void producerDeclarationsNoGroups() throws IOException {
		ConnectionFactory cf = mock(ConnectionFactory.class);
		Connection conn = mock(Connection.class);
		given(cf.createConnection()).willReturn(conn);
		Channel channel = mock(Channel.class);
		willReturn(new DeclareOk("x", 0, 0))
				.given(channel).queueDeclare(any(), eq(Boolean.TRUE), eq(Boolean.FALSE), eq(Boolean.FALSE), any());
		given(conn.createChannel(anyBoolean())).willReturn(channel);
		RabbitExchangeQueueProvisioner provisioner = new RabbitExchangeQueueProvisioner(cf);

		RabbitProducerProperties props = new RabbitProducerProperties();
		ExtendedProducerProperties<RabbitProducerProperties> properties =
				new ExtendedProducerProperties<RabbitProducerProperties>(props);
		ProducerDestination dest = provisioner.provisionProducerDestination("bar", properties);
		ApplicationContext ctx =
				TestUtils.getPropertyValue(provisioner, "autoDeclareContext", ApplicationContext.class);
		Set<String> declarables = ctx.getBeansOfType(Declarables.class).keySet();
		String qual = TestUtils.getPropertyValue(dest, "beanNameQualifier", String.class);
		assertThat(declarables).contains("bar." + qual + ".exchange");
		provisioner.cleanAutoDeclareContext(dest, properties);
		assertThat(ctx.getBeansOfType(Declarables.class)).isEmpty();
	}

	@Test
	void producerDeclarationsWithAlternateNoQueue() throws IOException {
		ConnectionFactory cf = mock(ConnectionFactory.class);
		Connection conn = mock(Connection.class);
		given(cf.createConnection()).willReturn(conn);
		Channel channel = mock(Channel.class);
		given(conn.createChannel(anyBoolean())).willReturn(channel);
		RabbitExchangeQueueProvisioner provisioner = new RabbitExchangeQueueProvisioner(cf);

		RabbitProducerProperties props = new RabbitProducerProperties();
		AlternateExchange alternate = new AlternateExchange();
		alternate.setName("altEx");
		props.setAlternateExchange(alternate);
		ExtendedProducerProperties<RabbitProducerProperties> properties =
				new ExtendedProducerProperties<RabbitProducerProperties>(props);
		ProducerDestination dest = provisioner.provisionProducerDestination("withAlt", properties);
		ApplicationContext ctx =
				TestUtils.getPropertyValue(provisioner, "autoDeclareContext", ApplicationContext.class);
		Map<String, Declarables> declarables = ctx.getBeansOfType(Declarables.class);
		assertThat(declarables).hasSize(2);
		String qual = TestUtils.getPropertyValue(dest, "beanNameQualifier", String.class);
		Declarables mainEx = declarables.get("withAlt." + qual + ".exchange");
		assertThat(mainEx).isNotNull();
		Exchange exch = (Exchange) mainEx.getDeclarables().iterator().next();
		assertThat(exch.getArguments().get("alternate-exchange")).isEqualTo("altEx");
		Declarables altEx = declarables.get("altEx." + qual + ".exchange");
		assertThat(altEx).isNotNull();
		exch = (Exchange) altEx.getDeclarables().iterator().next();
		assertThat(exch).isInstanceOf(TopicExchange.class);
		provisioner.cleanAutoDeclareContext(dest, properties);
		assertThat(ctx.getBeansOfType(Declarables.class)).isEmpty();
	}

	@Test
	void producerDeclarationsWithAlternateWithQueue() throws IOException {
		ConnectionFactory cf = mock(ConnectionFactory.class);
		Connection conn = mock(Connection.class);
		given(cf.createConnection()).willReturn(conn);
		Channel channel = mock(Channel.class);
		willReturn(new DeclareOk("x", 0, 0))
				.given(channel).queueDeclare(any(), eq(Boolean.TRUE), eq(Boolean.FALSE), eq(Boolean.FALSE), any());
		given(conn.createChannel(anyBoolean())).willReturn(channel);
		RabbitExchangeQueueProvisioner provisioner = new RabbitExchangeQueueProvisioner(cf);

		RabbitProducerProperties props = new RabbitProducerProperties();
		AlternateExchange alternate = new AlternateExchange();
		alternate.setName("altEx");
		alternate.setType(ExchangeTypes.DIRECT);
		Binding binding = new Binding();
		binding.setQueue("altQ");
		binding.setRoutingKey("altRK");
		alternate.setBinding(binding);
		props.setAlternateExchange(alternate);
		ExtendedProducerProperties<RabbitProducerProperties> properties =
				new ExtendedProducerProperties<RabbitProducerProperties>(props);
		ProducerDestination dest = provisioner.provisionProducerDestination("withAlt", properties);
		ApplicationContext ctx =
				TestUtils.getPropertyValue(provisioner, "autoDeclareContext", ApplicationContext.class);
		Map<String, Declarables> declarables = ctx.getBeansOfType(Declarables.class);
		assertThat(declarables).hasSize(4);
		String qual = TestUtils.getPropertyValue(dest, "beanNameQualifier", String.class);
		Declarables mainEx = declarables.get("withAlt." + qual + ".exchange");
		assertThat(mainEx).isNotNull();
		Exchange exch = (Exchange) mainEx.getDeclarables().iterator().next();
		assertThat(exch).isInstanceOf(TopicExchange.class);
		assertThat(exch.getArguments().get("alternate-exchange")).isEqualTo("altEx");
		Declarables altEx = declarables.get("altEx." + qual + ".exchange");
		assertThat(altEx).isNotNull();
		exch = (Exchange) altEx.getDeclarables().iterator().next();
		assertThat(exch).isInstanceOf(DirectExchange.class);
		Declarables queueDec = declarables.get("altEx.altQ." + qual);
		assertThat(queueDec).isNotNull();
		Declarables bindingDec = declarables.get("altEx.altQ." + qual + ".binding");
		assertThat(bindingDec).isNotNull();
		org.springframework.amqp.core.Binding bdg = (org.springframework.amqp.core.Binding) bindingDec.getDeclarables()
				.iterator().next();
		assertThat(bdg.getExchange()).isEqualTo("altEx");
		assertThat(bdg.getDestination()).isEqualTo("altQ");
		assertThat(bdg.getRoutingKey()).isEqualTo("altRK");
		provisioner.cleanAutoDeclareContext(dest, properties);
		assertThat(ctx.getBeansOfType(Declarables.class)).isEmpty();
	}

	@Test
	void producerDeclarationsWithGroupsAndDlq() throws IOException {
		ConnectionFactory cf = mock(ConnectionFactory.class);
		Connection conn = mock(Connection.class);
		given(cf.createConnection()).willReturn(conn);
		Channel channel = mock(Channel.class);
		willReturn(new DeclareOk("x", 0, 0))
				.given(channel).queueDeclare(any(), eq(Boolean.TRUE), eq(Boolean.FALSE), eq(Boolean.FALSE), any());
		given(conn.createChannel(anyBoolean())).willReturn(channel);
		RabbitExchangeQueueProvisioner provisioner = new RabbitExchangeQueueProvisioner(cf);

		RabbitProducerProperties props = new RabbitProducerProperties();
		props.setAutoBindDlq(true);
		ExtendedProducerProperties<RabbitProducerProperties> properties =
				new ExtendedProducerProperties<RabbitProducerProperties>(props);
		properties.setRequiredGroups("group1", "group2");
		ProducerDestination dest = provisioner.provisionProducerDestination("baz", properties);
		ApplicationContext ctx =
				TestUtils.getPropertyValue(provisioner, "autoDeclareContext", ApplicationContext.class);
		Set<String> declarables = ctx.getBeansOfType(Declarables.class).keySet();
		String qual = TestUtils.getPropertyValue(dest, "beanNameQualifier", String.class);
		assertThat(declarables).contains("baz." + qual + ".exchange", "baz.group1", "baz.group1.binding",
				"baz.group1.dlq", "DLX.group1.exchange", "baz.group1.dlq.binding", "baz.group2", "baz.group2.binding",
				"baz.group2.dlq", "DLX.group2.exchange", "baz.group2.dlq.binding");
		provisioner.cleanAutoDeclareContext(dest, properties);
		assertThat(ctx.getBeansOfType(Declarables.class)).isEmpty();
	}

	@Test
	void producerDeclarationsWithGroupsAndDlqAndPartitions() throws IOException {
		ConnectionFactory cf = mock(ConnectionFactory.class);
		Connection conn = mock(Connection.class);
		given(cf.createConnection()).willReturn(conn);
		Channel channel = mock(Channel.class);
		willReturn(new DeclareOk("x", 0, 0))
				.given(channel).queueDeclare(any(), eq(Boolean.TRUE), eq(Boolean.FALSE), eq(Boolean.FALSE), any());
		given(conn.createChannel(anyBoolean())).willReturn(channel);
		RabbitExchangeQueueProvisioner provisioner = new RabbitExchangeQueueProvisioner(cf);

		RabbitProducerProperties props = new RabbitProducerProperties();
		props.setAutoBindDlq(true);
		ExtendedProducerProperties<RabbitProducerProperties> properties =
				new ExtendedProducerProperties<RabbitProducerProperties>(props);
		properties.setRequiredGroups("group1", "group2");
		properties.setPartitionKeyExpression(new LiteralExpression("foo"));
		properties.setPartitionCount(2);
		ProducerDestination dest = provisioner.provisionProducerDestination("qux", properties);
		ApplicationContext ctx =
				TestUtils.getPropertyValue(provisioner, "autoDeclareContext", ApplicationContext.class);
		Set<String> declarables = ctx.getBeansOfType(Declarables.class).keySet();
		String qual = TestUtils.getPropertyValue(dest, "beanNameQualifier", String.class);
		assertThat(declarables).contains("qux." + qual + ".exchange", "qux.group1-0", "qux.group1-0.binding",
				"qux.group1-1", "qux.group1-1.binding", "qux.group1.dlq", "DLX.group1.exchange",
				"qux.group1.dlq.binding", "qux.group2-0",
				"qux.group2-0.binding", "qux.group2-1", "qux.group2-1.binding", "qux.group2.dlq", "DLX.group2.exchange",
				"qux.group2.dlq.binding");
		provisioner.cleanAutoDeclareContext(dest, properties);
		assertThat(ctx.getBeansOfType(Declarables.class)).isEmpty();
	}

	@Test
	void producerDeclarationsWithGroupsAndDlqAndPartitionsQueueNameIsGroup() throws IOException {
		ConnectionFactory cf = mock(ConnectionFactory.class);
		Connection conn = mock(Connection.class);
		given(cf.createConnection()).willReturn(conn);
		Channel channel = mock(Channel.class);
		willReturn(new DeclareOk("x", 0, 0))
				.given(channel).queueDeclare(any(), eq(Boolean.TRUE), eq(Boolean.FALSE), eq(Boolean.FALSE), any());
		given(conn.createChannel(anyBoolean())).willReturn(channel);
		RabbitExchangeQueueProvisioner provisioner = new RabbitExchangeQueueProvisioner(cf);

		RabbitProducerProperties props = new RabbitProducerProperties();
		props.setAutoBindDlq(true);
		props.setQueueNameGroupOnly(true);
		ExtendedProducerProperties<RabbitProducerProperties> properties =
				new ExtendedProducerProperties<RabbitProducerProperties>(props);
		properties.setRequiredGroups("group1", "group2");
		properties.setPartitionKeyExpression(new LiteralExpression("foo"));
		properties.setPartitionCount(2);
		ProducerDestination dest = provisioner.provisionProducerDestination("qux", properties);
		ApplicationContext ctx =
				TestUtils.getPropertyValue(provisioner, "autoDeclareContext", ApplicationContext.class);
		Set<String> declarables = ctx.getBeansOfType(Declarables.class).keySet();
		String qual = TestUtils.getPropertyValue(dest, "beanNameQualifier", String.class);
		assertThat(declarables).contains("qux." + qual + ".exchange", "group1-0", "group1-0.binding", "group1-1",
				"group1-1.binding", "group1.dlq", "DLX.group1.exchange", "group1.dlq.binding", "group2-0",
				"group2-0.binding", "group2-1", "group2-1.binding", "group2.dlq", "DLX.group2.exchange",
				"group2.dlq.binding");
		provisioner.cleanAutoDeclareContext(dest, properties);
		assertThat(ctx.getBeansOfType(Declarables.class)).isEmpty();
	}

}
