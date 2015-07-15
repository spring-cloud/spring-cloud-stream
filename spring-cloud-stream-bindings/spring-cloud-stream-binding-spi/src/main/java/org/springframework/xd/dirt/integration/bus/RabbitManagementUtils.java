/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.xd.dirt.integration.bus;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.HttpClient;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;

import org.springframework.http.HttpMethod;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

/**
 * @author Gary Russell
 * @since 1.2
 */
public class RabbitManagementUtils {

	public static RestTemplate buildRestTemplate(String adminUri, String user, String password) {
		BasicCredentialsProvider credsProvider = new BasicCredentialsProvider();
		credsProvider.setCredentials(
				new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT),
				new UsernamePasswordCredentials(user, password));
		HttpClient httpClient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
		// Set up pre-emptive basic Auth because the rabbit plugin doesn't currently support challenge/response for PUT
		// Create AuthCache instance
		AuthCache authCache = new BasicAuthCache();
		// Generate BASIC scheme object and add it to the local; from the apache docs...
		// auth cache
		BasicScheme basicAuth = new BasicScheme();
		URI uri;
		try {
			uri = new URI(adminUri);
		}
		catch (URISyntaxException e) {
			throw new RabbitAdminException("Invalid URI", e);
		}
		authCache.put(new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme()), basicAuth);
		// Add AuthCache to the execution context
		final HttpClientContext localContext = HttpClientContext.create();
		localContext.setAuthCache(authCache);
		RestTemplate restTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory(httpClient) {

			@Override
			protected HttpContext createHttpContext(HttpMethod httpMethod, URI uri) {
				return localContext;
			}

		});
		restTemplate.setMessageConverters(Collections.<HttpMessageConverter<?>>singletonList(
				new MappingJackson2HttpMessageConverter()));
		return restTemplate;
	}

}
