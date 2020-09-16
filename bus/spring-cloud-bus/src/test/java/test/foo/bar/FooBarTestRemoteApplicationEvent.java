/*
 * Copyright 2012-2019 the original author or authors.
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

package test.foo.bar;

import org.springframework.cloud.bus.event.RemoteApplicationEvent;

@SuppressWarnings("serial")
public class FooBarTestRemoteApplicationEvent extends RemoteApplicationEvent {

	@SuppressWarnings("unused")
	private FooBarTestRemoteApplicationEvent() {
	}

	protected FooBarTestRemoteApplicationEvent(final Object source, final String originService,
			final String destinationService) {
		super(source, originService, destinationService);
	}

	protected FooBarTestRemoteApplicationEvent(final Object source, final String originService) {
		super(source, originService);
	}

}
