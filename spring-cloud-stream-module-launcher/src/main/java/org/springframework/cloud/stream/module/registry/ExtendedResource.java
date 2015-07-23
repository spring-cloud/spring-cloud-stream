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

package org.springframework.cloud.stream.module.registry;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.util.FileSystemUtils;
import org.springframework.util.ReflectionUtils;

/**
 * A wrapper around a Resource that adds capabilities lacking in the original interface.
 * @author Eric Bottard
 * @author David Turanski
 * @since 1.2
 */
public abstract class ExtendedResource {

	private static final Logger log = LoggerFactory.getLogger(ExtendedResource.class);

	private static Class<?> HDFS_RESOURCE;

	static {
		try {
			HDFS_RESOURCE = Class.forName("org.springframework.data.hadoop.fs.HdfsResource");
		}
		catch (ClassNotFoundException e) {
			log.warn("Spring Data Hadoop is required on the classpath to register modules to HDFS. This feature is " +
					"disabled.");
		}
	}

	/**
	 * Delete the resource and all of its children, if any.
	 * @return whether deletion was successful or not
	 */
	public abstract boolean delete() throws IOException;

	/**
	 * Makes sure that the resource and all its parent "directories" exist, creating them if necessary.
	 */
	public abstract boolean mkdirs() throws IOException;

	public static ExtendedResource wrap(Resource original) {
		if (original instanceof FileSystemResource) {
			return new FileSystemExtendedResource((FileSystemResource) original);
		}
		else if (original.getClass().equals(HDFS_RESOURCE)) {
			return new HdfsExtendedResource(original);
		}
		else {
			throw new IllegalArgumentException("Unsupported resource " + original);
		}

	}

	public static class FileSystemExtendedResource extends ExtendedResource {
		private FileSystemResource fsResource;

		FileSystemExtendedResource(FileSystemResource fsResource) {

			this.fsResource = fsResource;
		}

		@Override
		public boolean delete() throws IOException {
			return FileSystemUtils.deleteRecursively(fsResource.getFile());
		}

		@Override
		public boolean mkdirs() throws IOException {
			return fsResource.getFile().mkdirs();
		}
	}

	public static class HdfsExtendedResource extends ExtendedResource {

		private static final Field PATH_FIELD;

		private static final Field FS_FIELD;

		static {
			PATH_FIELD = ReflectionUtils.findField(HDFS_RESOURCE, "path");
			ReflectionUtils.makeAccessible(PATH_FIELD);
			FS_FIELD = ReflectionUtils.findField(HDFS_RESOURCE, "fs");
			ReflectionUtils.makeAccessible(FS_FIELD);
		}


		private Path path;

		private FileSystem fs;

		HdfsExtendedResource(Resource hdfsResource) {
			path = (Path) ReflectionUtils.getField(PATH_FIELD, hdfsResource);
			fs = (FileSystem) ReflectionUtils.getField(FS_FIELD, hdfsResource);
		}

		@Override
		public boolean delete() throws IOException {
			return fs.delete(path, true);
		}

		@Override
		public boolean mkdirs() throws IOException {
			return fs.mkdirs(path);
		}
	}
}
