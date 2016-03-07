/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.binding;

import org.junit.Assert;
import org.junit.Test;

import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;


/**
 * @author Marius Bogoevici
 */
public class PropertiesClassResolutionTests {

	@Test
	public void testDefaultResolution() {
		SimpleBinderImplementation testBinder = new SimpleBinderImplementation();
		Assert.assertEquals(ConsumerProperties.class, ChannelBindingService.resolveConsumerPropertiesType(testBinder));
		Assert.assertEquals(ProducerProperties.class, ChannelBindingService.resolveProducerPropertiesType(testBinder));
	}

	@Test
	public void testBinderImplementorWithCustomTypes() {
		BinderImplementationWithCustomTypes testBinder = new BinderImplementationWithCustomTypes();
		Assert.assertEquals(SubclassConsumerProperties.class, ChannelBindingService.resolveConsumerPropertiesType(testBinder));
		Assert.assertEquals(SubclassProducerProperties.class, ChannelBindingService.resolveProducerPropertiesType(testBinder));
	}

	@Test
	public void testExtendsAbstractBinderWithDefaultTypes() {
		ExtendsAbstractBinderWithDefaultTypes testBinder = new ExtendsAbstractBinderWithDefaultTypes();
		Assert.assertEquals(ConsumerProperties.class, ChannelBindingService.resolveConsumerPropertiesType(testBinder));
		Assert.assertEquals(ProducerProperties.class, ChannelBindingService.resolveProducerPropertiesType(testBinder));
	}

	@Test
	public void testExtendsAbstractBinderWithCustomTypes() {
		ExtendsAbstractBinderWithCustomTypes testBinder = new ExtendsAbstractBinderWithCustomTypes();
		Assert.assertEquals(SubclassConsumerProperties.class, ChannelBindingService.resolveConsumerPropertiesType(testBinder));
		Assert.assertEquals(SubclassProducerProperties.class, ChannelBindingService.resolveProducerPropertiesType(testBinder));
	}

	@Test
	public void testGenericBinderWithDefaultTypes() {
		GenericBinderWithDefaultTypes testBinder = new GenericBinderWithDefaultTypes();
		Assert.assertEquals(ConsumerProperties.class, ChannelBindingService.resolveConsumerPropertiesType(testBinder));
		Assert.assertEquals(ProducerProperties.class, ChannelBindingService.resolveProducerPropertiesType(testBinder));
	}

	@Test
	public void testGenericBinderWithCustomTypes() {
		GenericBinderWithCustomTypes testBinder = new GenericBinderWithCustomTypes();
		Assert.assertEquals(SubclassConsumerProperties.class, ChannelBindingService.resolveConsumerPropertiesType(testBinder));
		Assert.assertEquals(SubclassProducerProperties.class, ChannelBindingService.resolveProducerPropertiesType(testBinder));
	}

	private class SimpleBinderImplementation implements Binder<Object, ConsumerProperties, ProducerProperties> {

		@Override
		public Binding<Object> bindConsumer(String name, String group, Object inboundBindTarget, ConsumerProperties consumerProperties) {
			return null;
		}

		@Override
		public Binding<Object> bindProducer(String name, Object outboundBindTarget, ProducerProperties producerProperties) {
			return null;
		}
	}

	private class BinderImplementationWithCustomTypes implements Binder<Object, SubclassConsumerProperties, SubclassProducerProperties> {

		@Override
		public Binding<Object> bindConsumer(String name, String group, Object inboundBindTarget, SubclassConsumerProperties consumerProperties) {
			return null;
		}

		@Override
		public Binding<Object> bindProducer(String name, Object outboundBindTarget, SubclassProducerProperties producerProperties) {
			return null;
		}
	}

	private class ExtendsAbstractBinderWithDefaultTypes extends AbstractBinder<Object, ConsumerProperties, ProducerProperties> {

		@Override
		protected Binding<Object> doBindConsumer(String name, String group, Object inputTarget, ConsumerProperties properties) {
			return null;
		}

		@Override
		protected Binding<Object> doBindProducer(String name, Object outboundBindTarget, ProducerProperties properties) {
			return null;
		}
	}

	private class ExtendsAbstractBinderWithCustomTypes extends AbstractBinder<Object, SubclassConsumerProperties, SubclassProducerProperties> {

		@Override
		protected Binding<Object> doBindConsumer(String name, String group, Object inputTarget, SubclassConsumerProperties properties) {
			return null;
		}

		@Override
		protected Binding<Object> doBindProducer(String name, Object outboundBindTarget, SubclassProducerProperties properties) {
			return null;
		}
	}

	private class GenericBinderWithDefaultTypes<C extends ConsumerProperties, P extends ProducerProperties> extends AbstractBinder<Object, C, P> {

		@Override
		protected Binding<Object> doBindConsumer(String name, String group, Object inputTarget, C properties) {
			return null;
		}

		@Override
		protected Binding<Object> doBindProducer(String name, Object outboundBindTarget, P properties) {
			return null;
		}
	}

	private class GenericBinderWithCustomTypes<C extends SubclassConsumerProperties, P extends SubclassProducerProperties> extends AbstractBinder<Object, C, P> {

		@Override
		protected Binding<Object> doBindConsumer(String name, String group, Object inputTarget, C properties) {
			return null;
		}

		@Override
		protected Binding<Object> doBindProducer(String name, Object outboundBindTarget, P properties) {
			return null;
		}
	}

	private class SubclassConsumerProperties extends ConsumerProperties {

	}

	private class SubclassProducerProperties extends ProducerProperties {

	}
}
