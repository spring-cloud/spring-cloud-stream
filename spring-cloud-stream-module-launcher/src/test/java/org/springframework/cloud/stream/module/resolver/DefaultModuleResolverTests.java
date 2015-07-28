/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.module.resolver;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.util.SocketUtils;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author David Turanski
 */
public class DefaultModuleResolverTests {
	private int port = SocketUtils.findAvailableTcpPort();

	@Rule
	public WireMockRule wireMockRule = new WireMockRule(port);

	@Test
	public void testResolveLocal() throws IOException {
		ClassPathResource cpr = new ClassPathResource("local-repo");
		File localRepository = cpr.getFile();
		DefaultModuleResolver defaultModuleResolver = new DefaultModuleResolver(localRepository, null);
		Resource resource = defaultModuleResolver.resolve("foo.bar", "foo-bar", "1.0.0");
		assertTrue(resource.exists());
		assertEquals(resource.getFile().getName(), "foo-bar-1.0.0.jar");
	}

	@Test(expected = RuntimeException.class)
	public void testResolveDoesNotExist() throws IOException {
		ClassPathResource cpr = new ClassPathResource("local-repo");
		File localRepository = cpr.getFile();
		DefaultModuleResolver defaultModuleResolver = new DefaultModuleResolver(localRepository, null);
		defaultModuleResolver.resolve("niente", "nada", "zilch");
	}

	@Test
	@Ignore
	public void testResolveRemote() throws IOException {
		ClassPathResource cpr = new ClassPathResource("local-repo");
		File localRepository = cpr.getFile();

		Map<String, String> remoteRepos = new HashMap<>();

		remoteRepos.put("spring", "http://repo.spring.io/release");
		remoteRepos.put("spring-snap", "http://repo.spring.io/snapshot");
		remoteRepos.put("spring-ms", "http://repo.spring.io/milestone");

		DefaultModuleResolver defaultModuleResolver = new DefaultModuleResolver(localRepository, remoteRepos);
		Resource resource = defaultModuleResolver.resolve("org.springframework", "spring-core", "4.1.6.RELEASE");
		assertTrue(resource.exists());
		assertEquals(resource.getFile().getName(), "spring-core-4.1.6.RELEASE.jar");
	}

	@Test
	public void testResolveMockRemote() throws IOException {
		ClassPathResource cpr = new ClassPathResource("local-repo");
		File localRepository = cpr.getFile();

		ClassPathResource stubJarResource = new ClassPathResource("__files/foo.jar");

		String stubFileName = stubJarResource.getFile().getName();

		Map<String, String> remoteRepos = new HashMap<>();

		remoteRepos.put("repo0", "http://localhost:" + port + "/repo0");
		remoteRepos.put("repo1", "http://localhost:" + port + "/repo1");

		stubFor(get(urlEqualTo("/repo1/org/bar/foo/1.0.0/foo-1.0.0.jar"))
				.willReturn(aResponse()
						.withStatus(200)
						.withBodyFile(stubFileName)));

		DefaultModuleResolver defaultModuleResolver = new DefaultModuleResolver(localRepository, remoteRepos);
		Resource resource = defaultModuleResolver.resolve("org.bar", "foo", "1.0.0");
		assertTrue(resource.exists());
		assertEquals(resource.getFile().getName(), "foo-1.0.0.jar");
	}
}
