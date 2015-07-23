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
 *
 */

package org.springframework.cloud.stream.module.registry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.util.FileCopyUtils;

/**
 * A {@link StalenessCheck} that expects to find a {@code .md5} file next to a
 * module archive and returns true if the computed hashes differ.
 * @author Eric Bottard
 * @see WritableResourceModuleRegistry
 * @since 1.2
 */
public class MD5StalenessCheck implements StalenessCheck {

	private ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

	@Override
	public boolean isStale(SimpleModuleDefinition targetDefinition, SimpleModuleDefinition sourceDefinition) {
		if (targetDefinition == null) {
			return true;
		}

		String md5SourceLocation = sourceDefinition.getLocation() + ".md5";
		String md5TargetLocation = targetDefinition.getLocation() + ".md5";

		String md5Source = readHash(md5SourceLocation);
		String md5Target = readHash(md5TargetLocation);

		return md5Target == null || md5Source == null || !md5Source.equals(md5Target);
	}

	private String readHash(String hashLocation) {
		Resource resource = resolver.getResource(hashLocation);
		if (resource == null || !resource.isReadable()) {
			return null;
		}
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			FileCopyUtils.copy(resource.getInputStream(), bos);
		}
		catch (IOException e) {
			throw new RuntimeException("Exception while trying to read hash at " + hashLocation, e);
		}
		return bos.toString();
	}
}