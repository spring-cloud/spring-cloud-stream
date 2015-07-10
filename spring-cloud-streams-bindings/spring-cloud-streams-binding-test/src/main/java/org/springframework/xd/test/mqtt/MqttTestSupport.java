/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.xd.test.mqtt;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.xd.test.AbstractExternalResourceTestSupport;

/**
 * @author Luke Taylor
 */
public class MqttTestSupport extends AbstractExternalResourceTestSupport<MqttClient> {

	public MqttTestSupport() {
		super("MQTT");
	}

	@Override
	protected void obtainResource() throws Exception {
		resource = new MqttClient("tcp://localhost:1883", "xd-test-" + System.currentTimeMillis(), new MemoryPersistence());
		resource.connect();
	}

	@Override
	protected void cleanupResource() throws Exception {
		resource.close();
	}

}
