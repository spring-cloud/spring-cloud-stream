/*
 * Copyright 2013-2015 the original author or authors.
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
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.WritableResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.data.hadoop.configuration.ConfigurationFactoryBean;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;

/**
 * Writable extension of {@link ResourceModuleRegistry}.
 * <p>Will generate MD5 hash files for written modules.</p>
 * @author Eric Bottard
 * @author David Turanski
 */
public class WritableResourceModuleRegistry extends ResourceModuleRegistry implements WritableModuleRegistry,
		InitializingBean {

	final protected static byte[] HEX_DIGITS = "0123456789ABCDEF".getBytes();

	final protected static String XD_CONFIG_HOME = "xd.config.home";

	/**
	 * Whether to attempt to create the directory structure at startup (disable for read-only implementations).
	 */
	private boolean createDirectoryStructure = true;

	private ConfigurableEnvironment environment;


	public WritableResourceModuleRegistry(String root) {
		super(root);
		setRequireHashFiles(true);
	}

	@Override
	public boolean delete(ModuleDefinition definition) {
		try {
			Resource archive = getResources(definition.getGroupId(), definition.getArtifactId(), 
					definition.getVersion(),
					ARCHIVE_AS_FILE_EXTENSION).iterator().next();
			if (archive instanceof WritableResource) {
				WritableResource writableResource = (WritableResource) archive;
				WritableResource hashResource = (WritableResource) hashResource(writableResource);
				// Delete hash first
				ExtendedResource.wrap(hashResource).delete();
				return ExtendedResource.wrap(writableResource).delete();
			}
			else {
				return false;
			}
		}
		catch (IOException e) {
			throw new RuntimeException("Exception while trying to delete module " + definition, e);
		}

	}

	@Override
	public boolean registerNew(ModuleDefinition definition) {
		if (!(definition instanceof UploadedModuleDefinition)) {
			return false;
		}
		UploadedModuleDefinition uploadedModuleDefinition = (UploadedModuleDefinition) definition;
		try {
			Resource archive = getResources(definition.getGroupId(), definition.getArtifactId(), 
					definition.getVersion(),
					ARCHIVE_AS_FILE_EXTENSION).iterator().next();
			if (archive instanceof WritableResource) {
				WritableResource writableResource = (WritableResource) archive;
				Assert.isTrue(!writableResource.exists(), "Could not install " + uploadedModuleDefinition + " at " +
						"location " + writableResource + " as that file already exists");

				MessageDigest md = MessageDigest.getInstance("MD5");
				DigestInputStream dis = new DigestInputStream(uploadedModuleDefinition.getInputStream(), md);
				FileCopyUtils.copy(dis, writableResource.getOutputStream());
				WritableResource hashResource = (WritableResource) hashResource(writableResource);
				// Write hash last
				FileCopyUtils.copy(bytesToHex(md.digest()), hashResource.getOutputStream());

				return true;
			}
			else {
				return false;
			}
		}
		catch (IOException | NoSuchAlgorithmException e) {
			throw new RuntimeException("Error trying to save " + uploadedModuleDefinition, e);
		}
	}

	public void setEnvironment(Environment environment) {
		this.environment = (ConfigurableEnvironment) environment;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (root.startsWith("hdfs:")) {
			boolean securityConfigProvided = false;
			String authMethod = "";
			String namenodePrincipal = "";
			String rmManagerPrincipal = "";
			String userPrincipal = "";
			String userKeytab = "";
			// try servers.yml first
			if (environment != null && environment.containsProperty("spring.hadoop.security.authMethod")) {
				securityConfigProvided = true;
				authMethod = environment.getProperty("spring.hadoop.security.authMethod");
				namenodePrincipal = environment.getProperty("spring.hadoop.security.namenodePrincipal");
				rmManagerPrincipal = environment.getProperty("spring.hadoop.security.rmManagerPrincipal");
				userPrincipal = environment.getProperty("spring.hadoop.security.userPrincipal");
				userKeytab = environment.getProperty("spring.hadoop.security.userKeytab");
			}
			// if needed check hadoop.properties
			if (!securityConfigProvided && environment != null) {
				String xdConfigHadoopProps = environment.getProperty(XD_CONFIG_HOME) + "hadoop.properties";
				Properties hadoopProps = null;
				try {
					hadoopProps = PropertiesLoaderUtils.loadProperties(new UrlResource(xdConfigHadoopProps));
				}
				catch (IOException ignore) {}
				if (hadoopProps != null && (hadoopProps.containsKey("spring.hadoop.security.authMethod") ||
						hadoopProps.containsKey("hadoop.security.authentication"))) {
					securityConfigProvided = true;
					authMethod = hadoopProps.getProperty("spring.hadoop.security.authMethod");
					if (!StringUtils.hasText(authMethod)) {
						authMethod = hadoopProps.getProperty("hadoop.security.authentication");
					}
					namenodePrincipal = hadoopProps.getProperty("spring.hadoop.security.namenodePrincipal");
					if (!StringUtils.hasText(namenodePrincipal)) {
						namenodePrincipal = hadoopProps.getProperty("dfs.namenode.kerberos.principal");
					}
					rmManagerPrincipal = hadoopProps.getProperty("spring.hadoop.security.rmManagerPrincipal");
					if (!StringUtils.hasText(rmManagerPrincipal)) {
						rmManagerPrincipal = hadoopProps.getProperty("yarn.resourcemanager.principal");
					}
					userPrincipal = hadoopProps.getProperty("spring.hadoop.security.userPrincipal");
					userKeytab = hadoopProps.getProperty("spring.hadoop.security.userKeytab");
				}
			}
			ConfigurationFactoryBean configurationFactoryBean = new ConfigurationFactoryBean();
			configurationFactoryBean.setRegisterUrlHandler(true);
			configurationFactoryBean.setFileSystemUri(root);
			if (securityConfigProvided && "kerberos".equals(authMethod)) {
				configurationFactoryBean.setSecurityMethod(authMethod);
				configurationFactoryBean.setNamenodePrincipal(namenodePrincipal);
				configurationFactoryBean.setRmManagerPrincipal(rmManagerPrincipal);
				configurationFactoryBean.setUserPrincipal(userPrincipal);
				configurationFactoryBean.setUserKeytab(userKeytab);
			}
			configurationFactoryBean.afterPropertiesSet();

			this.resolver = new HdfsResourceLoader(configurationFactoryBean.getObject());
		}

		if (createDirectoryStructure) {
			// Create intermediary folders
			//TODO: what should we do here?
//			for (String type : ModuleType.values()) {
//				Resource folder = getResources(type.name(), "", "").iterator().next();
//				if (!folder.exists()) {
//					ExtendedResource.wrap(folder).mkdirs();
//				}
//			}
		}

	}

	public void setCreateDirectoryStructure(boolean createDirectoryStructure) {
		this.createDirectoryStructure = createDirectoryStructure;
	}

	private byte[] bytesToHex(byte[] bytes) {
		byte[] hexChars = new byte[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = HEX_DIGITS[v >>> 4];
			hexChars[j * 2 + 1] = HEX_DIGITS[v & 0x0F];
		}
		return hexChars;
	}
}