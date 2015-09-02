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

package org.springframework.cloud.stream.module.utils;

/**
 * @author Marius Bogoevici
 */
public class ClassloaderUtils {

	/**
	 * Retrieves the extension classloader of the current JVM, if accessible. In general the extension classloader is
	 * found in a hierarchy as the parent of the application classloader. If such a hierarchy does not exist, it will
	 * return the application classloader itself.
	 *
	 * @return the classloader
	 */
	public static ClassLoader getExtensionClassloader() {
		ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
		// try to retrieve the extension classloader
		ClassLoader extensionClassLoader = systemClassLoader != null ? systemClassLoader.getParent() : null;
		// set the classloader for the module as the extension classloader if available
		// fall back to the system classloader (which can also be null) if not available
		return extensionClassLoader != null ? extensionClassLoader : systemClassLoader;
	}
}
