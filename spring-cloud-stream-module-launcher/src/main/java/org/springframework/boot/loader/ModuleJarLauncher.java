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

package org.springframework.boot.loader;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.boot.loader.archive.Archive;
import org.springframework.boot.loader.util.AsciiBytes;

/**
 * A (possibly temporary) alternative to {@link JarLauncher} that provides a
 * public {@link #launch(String[])} method.
 *
 * @author Mark Fisher
 */
public class ModuleJarLauncher extends ExecutableArchiveLauncher {

	private static final AsciiBytes LIB = new AsciiBytes("lib/");

	public ModuleJarLauncher(Archive archive) {
		super(archive);
	}

	@Override
	protected boolean isNestedArchive(Archive.Entry entry) {
		return !entry.isDirectory() && entry.getName().startsWith(LIB);
	}

	@Override
	protected void postProcessClassPathArchives(List<Archive> archives) throws Exception {
		archives.add(0, getArchive());
	}

	@Override
	public void launch(String[] args) {
		super.launch(args);
	}

	@Override
	protected ClassLoader createClassLoader(URL[] urls) throws Exception {
		return new LaunchedURLClassLoader(urls, null);
	}

}
