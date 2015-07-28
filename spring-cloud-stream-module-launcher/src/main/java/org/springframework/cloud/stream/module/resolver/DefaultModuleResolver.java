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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResolutionException;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.file.FileTransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * An implementation of ModuleResolver using <a href="http://www.eclipse.org/aether/>aether</a> resolve the module
 * artifact (uber jar) in a local Maven repository, downloading the latest update from a remote repository if
 * necessary.
 * @author David Turanski
 */
public class DefaultModuleResolver implements ModuleResolver {
	private static final String DEFAULT_CONTENT_TYPE = "default";

	private static final String DEFAULT_CLASSIFIER = "";

	private static final String DEFAULT_EXTENSION = "jar";

	private final File localRepository;

	private final List<RemoteRepository> remoteRepositories;

	private final RepositorySystem repositorySystem;

	@Override
	public Resource resolve(String groupId, String artifactId, String version) {
		Artifact artifact = new DefaultArtifact(groupId, artifactId, DEFAULT_CLASSIFIER, DEFAULT_EXTENSION, version);
		RepositorySystemSession session = newRepositorySystemSession(repositorySystem,
				localRepository.getAbsolutePath());
		ArtifactResult result;

		try {
			result = repositorySystem.resolveArtifact(session, new ArtifactRequest(artifact, remoteRepositories,
					"runtime"));
		}
		catch (ArtifactResolutionException e) {
			throw new RuntimeException(e);
		}

		return new FileSystemResource(result.getArtifact().getFile());
	}

	public DefaultModuleResolver(File localRepo, Map<String, String> remoteRepos) {
		Assert.isTrue(localRepo.exists(), "File " + localRepo + " does not exist.");
		this.localRepository = localRepo;
		this.remoteRepositories = new LinkedList<>();
		if (!CollectionUtils.isEmpty(remoteRepos)) {
			for (Map.Entry<String, String> remoteRepo : remoteRepos.entrySet()) {
				RemoteRepository remoteRepository = new RemoteRepository.Builder(remoteRepo.getKey(),
						DEFAULT_CONTENT_TYPE, remoteRepo.getValue()).build();
				this.remoteRepositories.add(remoteRepository);
			}
		}
		repositorySystem = newRepositorySystem();
	}

	private DefaultRepositorySystemSession newRepositorySystemSession(RepositorySystem system,
			String localRepoPath) {
		DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();

		LocalRepository localRepo = new LocalRepository(localRepoPath);
		session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));
		return session;
	}

	private RepositorySystem newRepositorySystem() {
		/*
		 * Aether's components implement org.eclipse.aether.spi.locator.Service to ease manual wiring and using the
    	 * prepopulated DefaultServiceLocator, we only need to register the repository connector and transporter
         * factories.
		 */
		DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
		locator.addService(RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
		locator.addService(TransporterFactory.class, FileTransporterFactory.class);
		locator.addService(TransporterFactory.class, HttpTransporterFactory.class);


		locator.setErrorHandler(new DefaultServiceLocator.ErrorHandler() {
			@Override
			public void serviceCreationFailed(Class<?> type, Class<?> impl, Throwable exception) {
				throw new RuntimeException(exception);
			}
		});

		return locator.getService(RepositorySystem.class);
	}
}
