/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.metrics;

import org.junit.Assert;
import org.junit.Test;

import org.springframework.boot.bind.RelaxedNames;

/**
 * @author Vinicius Carvalho
 */
public class RelaxedPropertiesUtilsTests {

	@Test
	public void testVariations() throws Exception {
		RelaxedNames javaHome = new RelaxedNames("JAVA_HOME");
		RelaxedNames os = new RelaxedNames("OS");
		RelaxedNames springEnv = new RelaxedNames("SPRING_APPLICATION_NAME");
		RelaxedNames springDot = new RelaxedNames("spring.application.name");
		RelaxedNames springCamel = new RelaxedNames("springApplicationName");
		RelaxedNames contentType = new RelaxedNames("spring.cloud.stream.bindings.streamMetrics.contentType");
		RelaxedNames contentTypeEnv = new RelaxedNames("SPRING_CLOUD_STREAM_BINDINGS_STREAM-METRICS_CONTENT-TYPE");
		RelaxedNames xyz = new RelaxedNames("My.X.Is");
		RelaxedNames springMetrics = new RelaxedNames("spring.cloud.stream.streamMetrics");
		RelaxedNames springMetricsEnv = new RelaxedNames("spring.cloud.stream.stream-metrics");
		Assert.assertEquals("java.home", RelaxedPropertiesUtils.findCanonicalFormat(javaHome));
		Assert.assertEquals("os", RelaxedPropertiesUtils.findCanonicalFormat(os));
		Assert.assertEquals("spring.application.name", RelaxedPropertiesUtils.findCanonicalFormat(springEnv));
		Assert.assertEquals("spring.application.name", RelaxedPropertiesUtils.findCanonicalFormat(springDot));
		Assert.assertEquals("spring.application.name", RelaxedPropertiesUtils.findCanonicalFormat(springCamel));
		Assert.assertEquals("spring.cloud.stream.bindings.streamMetrics.contentType",
				RelaxedPropertiesUtils.findCanonicalFormat(contentType));
		Assert.assertEquals("spring.cloud.stream.bindings.streamMetrics.contentType",
				RelaxedPropertiesUtils.findCanonicalFormat(contentTypeEnv));
		Assert.assertEquals("My.X.Is", RelaxedPropertiesUtils.findCanonicalFormat(xyz));
		Assert.assertEquals("spring.cloud.stream.streamMetrics",
				RelaxedPropertiesUtils.findCanonicalFormat(springMetrics));
		Assert.assertEquals("spring.cloud.stream.streamMetrics",
				RelaxedPropertiesUtils.findCanonicalFormat(springMetricsEnv));
	}

}
