/*
 * Copyright 2013-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder.test.junit.rabbit;



import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.springframework.util.Assert;


/**
 * Abstract base class for JUnit {@link Rule}s that detect the presence of some external
 * resource. If the resource is indeed present, it will be available during the test
 * lifecycle through {@link #getResource()}. If it is not, tests will either fail or be
 * skipped, depending on the value of system property
 * {@value #SCS_EXTERNAL_SERVERS_REQUIRED}.
 *
 * @param <R> resource type
 * @author Eric Bottard
 * @author Gary Russell
 */
public abstract class AbstractExternalResourceTestSupport<R> implements BeforeEachCallback {

	/**
	 * SCS external servers required environment variable.
	 */
	public static final String SCS_EXTERNAL_SERVERS_REQUIRED = "SCS_EXTERNAL_SERVERS_REQUIRED";

	protected final Log logger = LogFactory.getLog(getClass());

	protected R resource;

	private String resourceDescription;

	protected AbstractExternalResourceTestSupport(String resourceDescription) {
		Assert.hasText(resourceDescription, "resourceDescription is required");
		this.resourceDescription = resourceDescription;
	}

	@Override
	public void beforeEach(ExtensionContext context) throws Exception {
		try {
			obtainResource();
		}
		catch (Exception e) {
			maybeCleanup();

			Assertions.fail("");
		}
	}

	private void maybeCleanup() {
		if (this.resource != null) {
			try {
				cleanupResource();
			}
			catch (Exception ignored) {
				this.logger.warn("Exception while trying to cleanup failed resource",
						ignored);
			}
		}
	}

	public R getResource() {
		return this.resource;
	}

	/**
	 * Perform cleanup of the {@link #resource} field, which is guaranteed to be non null.
	 * @throws Exception any exception thrown by this method will be logged and swallowed
	 */
	protected abstract void cleanupResource() throws Exception;

	/**
	 * Try to obtain and validate a resource. Implementors should either set the
	 * {@link #resource} field with a valid resource and return normally, or throw an
	 * exception.
	 * @throws Exception when resource couldn't be obtained
	 */
	protected abstract void obtainResource() throws Exception;

}
