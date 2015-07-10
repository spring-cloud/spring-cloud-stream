/*
 * Copyright 2013-2015 the original author or authors.
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

package org.springframework.xd.test.rabbit;


import java.net.Socket;

import javax.net.SocketFactory;

import org.junit.Rule;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.xd.test.AbstractExternalResourceTestSupport;

/**
 * JUnit {@link Rule} that detects the fact that RabbitMQ is available on localhost.
 *
 * @author Mark Fisher
 * @author Gary Russell
 * @author Eric Bottard
 */
public class RabbitTestSupport extends AbstractExternalResourceTestSupport<CachingConnectionFactory> {

	private final boolean management;

	public RabbitTestSupport() {
		this(false);
	}

	public RabbitTestSupport(boolean management) {
		super("RABBIT");
		this.management = management;
	}

	@Override
	protected void obtainResource() throws Exception {
		resource = new CachingConnectionFactory("localhost");
		resource.createConnection().close();
		if (management) {
			Socket socket = SocketFactory.getDefault().createSocket("localhost", 15672);
			socket.close();
		}
	}

	@Override
	protected void cleanupResource() throws Exception {
		resource.destroy();
	}

}
