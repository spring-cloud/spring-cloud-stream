/*
 * Copyright 2016-2019 the original author or authors.
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

package org.springframework.cloud.stream.config;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;

/**
 * @author Ilayaperumal Gopinathan
 */
public class StreamListenerTestUtils {

	public interface FooInboundChannel1 {

		String INPUT = "foo1-input";

		@Input(FooInboundChannel1.INPUT)
		SubscribableChannel input();

	}

	public interface FooOutboundChannel1 {

		String OUTPUT = "foo1-output";

		@Output(FooOutboundChannel1.OUTPUT)
		MessageChannel output();

	}

	public static class FooPojo {

		private String foo;

		public String getFoo() {
			return this.foo;
		}

		public void setFoo(String foo) {
			this.foo = foo;
		}

		@Override
		public String toString() {
			final StringBuffer sb = new StringBuffer("FooPojo{");
			sb.append("foo='").append(this.foo).append('\'');
			sb.append('}');
			return sb.toString();
		}

	}

	public static class BarPojo {

		private String bar;

		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

		@Override
		public String toString() {
			final StringBuffer sb = new StringBuffer("BarPojo{");
			sb.append("bar='").append(this.bar).append('\'');
			sb.append('}');
			return sb.toString();
		}

	}

	public static class PojoWithValidation {

		private String foo;

		public String getFoo() {
			return this.foo;
		}

		public void setFoo(String foo) {
			this.foo = foo;
		}

	}

}
