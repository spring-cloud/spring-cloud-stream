/*
 * Copyright 2013-2017 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.converter.ContentTypeResolver;

/**
 * @author Gary Russell
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 * @deprecated Serialization and conversion happens at the converters now, this test is no longer required
 */

public class MessageChannelBinderSupportTests {

	private final ContentTypeResolver contentTypeResolver = new StringConvertingContentTypeResolver();

	private final TestMessageChannelBinder binder = new TestMessageChannelBinder();


	public static class Foo {

		private String bar;

		public Foo() {
		}

		public Foo(String bar) {
			this.bar = bar;
		}

		public String getBar() {
			return bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

	}

	public static class Bar {

		private String foo;

		public Bar() {
		}

		public Bar(String foo) {
			this.foo = foo;
		}

		public String getFoo() {
			return foo;
		}

		public void setFoo(String foo) {
			this.foo = foo;
		}

	}

	public class TestMessageChannelBinder
			extends AbstractBinder<MessageChannel, ConsumerProperties, ProducerProperties> {

		@Override
		protected Binding<MessageChannel> doBindConsumer(String name, String group, MessageChannel channel,
				ConsumerProperties properties) {
			return null;
		}

		@Override
		public Binding<MessageChannel> doBindProducer(String name, MessageChannel channel,
				ProducerProperties properties) {
			return null;
		}
	}

}
