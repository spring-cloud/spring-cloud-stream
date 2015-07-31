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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.boot.loader.ModuleJarLauncher;
import org.springframework.boot.loader.archive.JarFileArchive;
import org.springframework.cloud.stream.module.resolver.AetherModuleResolver;
import org.springframework.cloud.stream.module.resolver.ModuleResolver;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Bootstrap for launching one or more modules. The module coordinates must be provided via the "modules" system
 * property or "MODULES" environment variable as a comma-delimited list. The format of each module must conform to the
 * <a href="http://www.eclipse.org/aether">Aether</a> convention:
 * <code>&lt;groupId&gt;:&lt;artifactId&gt;[:&lt;extension&gt;[:&lt;classifier&gt;]]:&lt;version&gt;</code> 
 * <p>
 * To pass args to a module, prefix with the module name and a dot. The arg name will be de-qualified and passed along.
 * For example: <code>--foo.bar=123</code> becomes <code>--bar=123</code> and is only passed to the 'foo' module.
 *
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 */
public class ModuleLauncher {

	// TODO ensure that this properly supports Windows too
	private static final String DEFAULT_LOCAL_REPO =
			System.getProperty("user.home") + File.separator  + ".m2" + File.separator +  "repository";

	private static final String DEFAULT_EXTENSION = "jar";

	private static final String DEFAULT_CLASSIFIER = "exec";

	private static final Pattern COORDINATES_PATTERN =
			Pattern.compile("([^: ]+):([^: ]+)(:([^: ]*)(:([^: ]+))?)?:([^: ]+)");

	private final ModuleResolver moduleResolver;

	public ModuleLauncher() {
		this(determineLocalRepositoryLocation(),
				Collections.singletonMap("spring-cloud-stream-modules", "http://repo.spring.io/spring-cloud-stream-modules"));
	}

	public ModuleLauncher(String localRepository, Map<String, String> remoteRepositories) {
		this.moduleResolver = new AetherModuleResolver(new File(localRepository), remoteRepositories);
	}

	public void launch(String[] modules, String[] args) {
		for (String module : modules) {
			List<String> moduleArgs = new ArrayList<>(); 
			for (String arg : args) {
				if (arg.startsWith("--" + module + ".")) {
					moduleArgs.add("--" + arg.substring(module.length() + 3));
				}
			}
			moduleArgs.add("--spring.jmx.default-domain=" + module.replace("/", ".").replace(":", "."));
			launchModule(module, moduleArgs.toArray(new String[moduleArgs.size()]));
		}
	}

	private void launchModule(String module, String[] args) {
		try {
			Resource resource = resolveModule(module);
			JarFileArchive jarFileArchive = new JarFileArchive(resource.getFile());
			ModuleJarLauncher jarLauncher = new ModuleJarLauncher(jarFileArchive);
			jarLauncher.launch(args);
		}
		catch (IOException e) {
			throw new RuntimeException("failed to launch module: " + module, e);
		}
	}

	private Resource resolveModule(String coordinates) {
		Matcher matcher = COORDINATES_PATTERN.matcher(coordinates);
		Assert.isTrue(matcher.matches(), "Bad artifact coordinates " + coordinates
				+ ", expected format is <groupId>:<artifactId>[:<extension>[:<classifier>]]:<version>");
		String groupId = matcher.group(1);
		String artifactId = matcher.group(2);
		String extension = StringUtils.hasLength(matcher.group(4)) ? matcher.group(4) : DEFAULT_EXTENSION;
		String classifier = StringUtils.hasLength(matcher.group(6)) ? matcher.group(6) : DEFAULT_CLASSIFIER;
		String version = matcher.group(7);
		return moduleResolver.resolve(groupId, artifactId, extension, classifier, version);
	}

	private static String determineLocalRepositoryLocation() {
		String localRepository = System.getProperty("local.repository");
		if (localRepository == null) {
			localRepository = System.getenv("LOCAL_REPOSITORY");
		}
		if (localRepository == null) {
			localRepository = DEFAULT_LOCAL_REPO;
		}
		return localRepository;
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
}
