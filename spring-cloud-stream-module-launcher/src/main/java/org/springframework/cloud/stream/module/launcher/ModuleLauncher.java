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

package org.springframework.cloud.stream.module.launcher;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.springframework.boot.loader.ModuleJarLauncher;
import org.springframework.boot.loader.archive.JarFileArchive;
import org.springframework.cloud.stream.module.resolver.AetherModuleResolver;
import org.springframework.cloud.stream.module.resolver.ModuleResolver;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

/**
 * Bootstrap for launching one or more modules. The module coordinates (as 'groupId:artifactId:version') must be
 * provided via the "modules" system property or "MODULES" environment variable as a comma-delimited list.
 * <p>
 * To pass args to a module, prefix with the module name and a dot. The arg name will be de-qualified and passed along.
 * For example: <code>--foo.bar=123</code> becomes <code>--bar=123</code> and is only passed to the 'foo' module.
 *
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
public class ModuleLauncher {

	private static final String LOCAL_REPO = "/opt/spring/modules";

	private final ModuleResolver moduleResolver;

	public ModuleLauncher() {
		this(Collections.singletonMap("spring-cloud-stream-modules", "http://repo.spring.io/spring-cloud-stream-modules"));
	}

	public ModuleLauncher(Map<String, String> remoteRepositories) {
		this.moduleResolver = new AetherModuleResolver(new File(LOCAL_REPO), remoteRepositories);
	}

	public void launch(String[] modules, String[] args) {
		Executor executor = Executors.newFixedThreadPool(modules.length);
		for (String module : modules) {
			List<String> moduleArgs = new ArrayList<>(); 
			for (String arg : args) {
				if (arg.startsWith("--" + module + ".")) {
					moduleArgs.add("--" + arg.substring(module.length() + 3));
				}
			}
			moduleArgs.add("--spring.jmx.default-domain=" + module.replace("/", ".").replace(":", "."));
			executor.execute(new ModuleLaunchTask(moduleResolver, module, moduleArgs.toArray(new String[moduleArgs.size()])));
		}
	}

	public static void main(String[] args) throws Exception {
		String modules = System.getProperty("modules");
		if (modules == null) {
			modules = System.getenv("MODULES");
		}
		if (modules == null) {
			System.err.println("Either the 'modules' system property or 'MODULES' environment variable is required.");
			System.exit(1);
		}
		ModuleLauncher launcher = new ModuleLauncher();
		launcher.launch(modules.split(","), args);
	}


	private static class ModuleLaunchTask implements Runnable {

		private final ModuleResolver moduleResolver;

		private final String groupId;

		private final String artifactId;

		private final String version;

		private final String[] args;

		ModuleLaunchTask(ModuleResolver moduleResolver, String module, String[] args) {
			this.moduleResolver = moduleResolver;
			String[] tokens = module.split(":");
			Assert.isTrue(tokens.length == 3, "required module format is: 'groupId:artifactId:version'");
			this.groupId = tokens[0];
			this.artifactId = tokens[1];
			this.version = tokens[2];
			this.args = args;
		}

		@Override
		public void run() {
			try {
				Resource resource = moduleResolver.resolve(groupId, artifactId, version, "exec", "jar");
				JarFileArchive jarFileArchive = new JarFileArchive(resource.getFile());
				ModuleJarLauncher jarLauncher = new ModuleJarLauncher(jarFileArchive);
				jarLauncher.launch(args);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
