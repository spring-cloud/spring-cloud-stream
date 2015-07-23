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

package org.springframework.cloud.stream.module.registry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.util.StringUtils;

/**
 * {@link Resource} based implementation of {@link ModuleRegistry}. <p>While not being coupled to {@code java.io.File}
 * access, will try to sanitize file paths as much as possible.</p>
 * @author Eric Bottard
 * @author David Turanski
 */
public class ResourceModuleRegistry implements ModuleRegistry {

	/**
	 * The extension the module 'File' must have if it's not in exploded dir format.
	 */
	public static final String ARCHIVE_AS_FILE_EXTENSION = ".jar";

	/**
	 * The extension to use for storing an uploaded module hash.
	 */
	public static final String HASH_EXTENSION = ".md5";

	private final static String[] SUFFIXES = new String[] {"", ARCHIVE_AS_FILE_EXTENSION};

	/**
	 * Whether to require the presence of a hash file for archives. Helps preventing half-uploaded archives from being
	 * picked up.
	 */
	private boolean requireHashFiles;

	protected ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

	protected String root;

	public ResourceModuleRegistry(String root) {
		this.root = StringUtils.trimTrailingCharacter(root, '/');
	}

	protected Resource hashResource(Resource moduleResource) throws IOException {
		// Yes, we'd like to use Resource.createRelative() but they don't all behave in the same way...
		String location = moduleResource.getURI().toString() + HASH_EXTENSION;
		return sanitize(resolver.getResources(location)).iterator().next();
	}


	@Override
	public List<ModuleDefinition> findVersionsForModuleDefinition(String groupId, String artifactId) {
		List<ModuleDefinition> result = new ArrayList<ModuleDefinition>();
		try {
			for (String suffix : SUFFIXES) {
				for (Resource resource : getResources(groupId, artifactId, "*", suffix)) {
					collect(resource, result);
				}
			}
		}
		catch (IOException e) {
			return Collections.emptyList();
		}
		return result;
	}

	@Override
	public List<ModuleDefinition> findModuleDefinitionsByGroupId(String groupId) {
		List<ModuleDefinition> result = new ArrayList<>();
		try {
			for (Resource resource : getResources(groupId, "*", "*", "")) {
				collect(resource, result);
			}
		}
		catch (IOException e) {
			return Collections.emptyList();
		}
		return result;
	}

	protected Iterable<Resource> getResources(String groupId,
			String artifactId, String version, String suffix) throws IOException {
		//TODO: this need to be fixed
		String path = String.format("%s/%s/%s%s", this.root, groupId, artifactId, version, suffix);
		Resource[] resources = this.resolver.getResources(path);
		List<Resource> filtered = sanitize(resources);

		return filtered;
	}

	private List<Resource> sanitize(Resource[] resources) throws IOException {
		List<Resource> filtered = new ArrayList<>();

		for (Resource resource : resources) {
			// Sanitize file paths (and force use of FSR, which is WritableResource)
			if (resource instanceof UrlResource && resource.getURL().getProtocol().equals("file")
					|| resource instanceof FileSystemResource) {
				resource = new FileSystemResource(resource.getFile().getCanonicalFile());
			}

			String fileName = resource.getFilename();
			if (!fileName.startsWith(".") &&
					(!fileName.contains(".") ||
							fileName.endsWith(ARCHIVE_AS_FILE_EXTENSION) ||
							fileName.endsWith(HASH_EXTENSION))) {
				filtered.add(resource);
			}
		}
		return filtered;
	}

	protected void collect(Resource resource, List<ModuleDefinition> holder) throws IOException {
		// ResourcePatternResolver.getResources() may return resources that don't exist when not using wildcards
		if (!resource.exists()) {
			return;
		}

		String path = resource.getURI().toString();
		if (path.endsWith(HASH_EXTENSION)) {
			return;
		}
		else if (path.endsWith(ARCHIVE_AS_FILE_EXTENSION) && requireHashFiles && !hashResource(resource).exists()) {
			return;
		}
		// URI paths for directories include an extra slash, strip it for now
		if (path.endsWith("/")) {
			path = path.substring(0, path.length() - 1);
		}

		//TODO: Parsing needs work here
		int lastSlash = path.lastIndexOf('/');
		int nextToLastSlash = path.lastIndexOf('/', lastSlash - 1);

		String groupId = path.substring(lastSlash + 1);
		if (groupId.endsWith(ARCHIVE_AS_FILE_EXTENSION)) {
			groupId = groupId.substring(0, groupId.length() - ARCHIVE_AS_FILE_EXTENSION.length());
		}
		String artifactId = path.substring(nextToLastSlash + 1, lastSlash);

		String locationToUse = resource.getURL().toString();
		if (!locationToUse.endsWith(ARCHIVE_AS_FILE_EXTENSION) && !locationToUse.endsWith("/")) {
			locationToUse += "/";
		}

		String version = null;

		SimpleModuleDefinition found = ModuleDefinitions.simple(groupId, artifactId, version, locationToUse);

		if (holder.contains(found)) {
			SimpleModuleDefinition other = (SimpleModuleDefinition) holder.get(holder.indexOf(found));
			throw new IllegalStateException(String.format("Duplicate module definitions for '%s:%s:%s' found at '%s'" +
							" " +
							"and" +
							" " +
							"'%s'",
					found.getGroupId(), found.getArtifactId(), found.getVersion(), found.getLocation(), 
					other.getLocation()));
		}
		else {
			holder.add(found);
		}
	}


	public void setRequireHashFiles(boolean requireHashFiles) {
		this.requireHashFiles = requireHashFiles;
	}

	@Override
	public ModuleDefinition findModuleDefinition(String groupId, String artifactId, String version) {
		List<ModuleDefinition> result = new ArrayList<ModuleDefinition>();
		try {
			for (String suffix : SUFFIXES) {
				for (Resource resource : getResources(groupId, artifactId, version, suffix)) {
					collect(resource, result);
				}
			}
		}
		catch (IOException e) {
			throw new RuntimeException(String.format("An error occurred trying to locate module '%s:%s:%s'",
					groupId, artifactId, version), e);
		}

		return result.size() == 1 ? result.iterator().next() : null;

	}

}
