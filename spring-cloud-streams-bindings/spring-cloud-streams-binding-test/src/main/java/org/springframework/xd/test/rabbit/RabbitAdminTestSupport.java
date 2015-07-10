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

package org.springframework.xd.test.rabbit;


import java.util.Map;

import org.junit.Rule;

import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.xd.test.AbstractExternalResourceTestSupport;

/**
 * JUnit {@link Rule} that detects the fact that RabbitMQ is available on localhost with
 * the management plugin enabled.
 *
 * @author Gary Russell
 * @since 1.2
 */
public class RabbitAdminTestSupport extends AbstractExternalResourceTestSupport<RestTemplate> {

	public RabbitAdminTestSupport() {
		super("RABBITADMIN");
	}

	@Override
	protected void obtainResource() throws Exception {
		resource = new RestTemplate();
		try {
			resource.getForObject("http://localhost:15672/api/overview", Map.class);
		}
		catch (HttpClientErrorException e) {
			if (e.getStatusCode() != HttpStatus.UNAUTHORIZED) {
				throw e;
			}
		}
	}

	@Override
	protected void cleanupResource() throws Exception {
	}

}
