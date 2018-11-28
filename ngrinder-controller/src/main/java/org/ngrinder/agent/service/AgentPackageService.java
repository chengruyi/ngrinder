package org.ngrinder.agent.service;

import freemarker.template.Configuration;
import freemarker.template.DefaultObjectWrapper;
import freemarker.template.Template;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.ngrinder.agent.model.NGrinderPackage;
import org.ngrinder.infra.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.apache.commons.lang.StringUtils.isNotEmpty;
import static org.ngrinder.agent.model.NGrinderPackage.AGENT;
import static org.ngrinder.common.util.CompressionUtils.*;
import static org.ngrinder.common.util.ExceptionUtils.processException;

/**
 * Agent package service.
 *
 * @author Matt
 * @since 3.3
 */
@Service
public class AgentPackageService {
	protected static final Logger LOGGER = LoggerFactory.getLogger(AgentPackageService.class);
	public static final int EXEC = 0x81ed;

	@Autowired
	private Config config;

	/**
	 * Create package from NGrinderPackage.
	 *
	 * @return File package.
	 */
	public File createPackage(NGrinderPackage nGrinderPackage, URLClassLoader classLoader, String regionName, String connectionIP, int port, String owner) {
		synchronized (AgentPackageService.class) {
			File packageTar = nGrinderPackage.makePackageTar(regionName, connectionIP, owner, false);
			if (packageTar.exists()) {
				return packageTar;
			}
			FileUtils.deleteQuietly(packageTar);
			try (TarArchiveOutputStream tarOutputStream = createTarArchiveStream(packageTar)) {
				addDependentLibToTarStream(nGrinderPackage, tarOutputStream, classLoader, nGrinderPackage.getBasePath(), nGrinderPackage.getLibPath(), nGrinderPackage.getShellScriptsPath());
				if (!nGrinderPackage.equals(AGENT) || isNotEmpty(connectionIP)) {
					addConfigToTar(nGrinderPackage, tarOutputStream, nGrinderPackage.getConfigParam(regionName, connectionIP, port, owner));
				}
			} catch (Exception e) {
				LOGGER.error("Error while generating an agent package" + e.getMessage());
			}
			return packageTar;
		}
	}

	/**
	 * Create agent package.
	 *
	 * @return File  agent package.
	 */
	public File createAgentPackage() {
		return createAgentPackage(AGENT, null, null, config.getControllerPort(), null);
	}

	/**
	 * Create agent package.
	 *
	 * @param connectionIP host ip.
	 * @param region       region
	 * @param owner        owner
	 * @return File  agent package.
	 */
	public File createAgentPackage(NGrinderPackage NGrinderPackage, String region, String connectionIP, int port, String owner) {
		synchronized (AgentPackageService.class) {
			if (!NGrinderPackage.equals(AGENT)) {
				NGrinderPackage = AGENT;
			}
			return createPackage(NGrinderPackage, (URLClassLoader) getClass().getClassLoader(), region, connectionIP, port, owner);
		}
	}

	private void addDependentLibToTarStream(NGrinderPackage NGrinderPackage, TarArchiveOutputStream tarOutputStream, URLClassLoader classLoader, String basePath, String libPath, String copyShellPath) throws IOException {
		if (classLoader == null) {
			classLoader = (URLClassLoader) getClass().getClassLoader();
		}
		addFolderToTar(tarOutputStream, basePath);
		addFolderToTar(tarOutputStream, libPath);
		Set<String> libs = NGrinderPackage.getPackageDependentLibs(classLoader);
		copyShellFile(tarOutputStream, basePath, copyShellPath);

		for (URL eachUrl : classLoader.getURLs()) {
			File eachClassPath = new File(eachUrl.getFile());
			if (!isJar(eachClassPath)) {
				continue;
			}
			if (isDependentLib(eachClassPath, libs)) {
				addFileToTar(tarOutputStream, eachClassPath, libPath + eachClassPath.getName());
			}
		}
	}

	private void copyShellFile(TarArchiveOutputStream tarOutputStream, String basePath, String copyShellPath) throws IOException {
		ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
		Resource[] resources = resolver.getResources(copyShellPath);
		InputStream shellFileIs;
		byte[] shellFileBytes;
		for (Resource resource : resources) {
			shellFileIs = resource.getInputStream();
			shellFileBytes = IOUtils.toByteArray(shellFileIs);
			IOUtils.closeQuietly(shellFileIs);
			addByteToTar(tarOutputStream, shellFileBytes, basePath + FilenameUtils.getName(resource.getFilename()),
				shellFileBytes.length, EXEC);
		}
	}

	private TarArchiveOutputStream createTarArchiveStream(File agentTar) throws IOException {
		FileOutputStream fos = new FileOutputStream(agentTar);
		return new TarArchiveOutputStream(new BufferedOutputStream(fos));
	}

	private void addConfigToTar(NGrinderPackage NGrinderPackage, TarArchiveOutputStream tarOutputStream, Map<String, Object> agentConfigParam) throws IOException {
		final String config = getConfigContent(NGrinderPackage.getTemplateName(), agentConfigParam);
		final byte[] bytes = config.getBytes();
		addInputStreamToTar(tarOutputStream, new ByteArrayInputStream(bytes), NGrinderPackage.getBasePath() + "__agent.conf",
			bytes.length, TarArchiveEntry.DEFAULT_FILE_MODE);
	}

	/**
	 * Check if this given path is jar.
	 *
	 * @param libFile lib file
	 * @return true if it's jar
	 */
	public boolean isJar(File libFile) {
		return libFile.getName().endsWith(".jar");
	}

	/**
	 * Check if this given lib file in the given lib set.
	 *
	 * @param libFile lib file
	 * @param libs    lib set
	 * @return true if dependent lib
	 */
	private boolean isDependentLib(File libFile, Set<String> libs) {
		if (libFile.getName().contains("grinder-3.9.1.jar")) {
			return false;
		}
		String name = libFile.getName().replace("-SNAPSHOT", "").replace("-GA", "");
		final int libVersionStartIndex = name.lastIndexOf("-");
		name = name.substring(0, (libVersionStartIndex == -1) ? name.lastIndexOf(".") : libVersionStartIndex);
		return libs.contains(name);
	}

	/**
	 * Get the agent.config content replacing the variables with the given values.
	 *
	 * @param templateName template name.
	 * @param values       map of configurations.
	 * @return generated string
	 */
	public String getConfigContent(String templateName, Map<String, Object> values) {
		try (StringWriter writer = new StringWriter()) {
			Configuration config = new Configuration();
			config.setClassForTemplateLoading(this.getClass(), "/ngrinder_agent_home_template");
			config.setObjectWrapper(new DefaultObjectWrapper());
			Template template = config.getTemplate(templateName);
			template.process(values, writer);
			return writer.toString();
		} catch (Exception e) {
			throw processException("Error while fetching the script template.", e);
		}
	}

	static class TarArchivingZipEntryProcessor implements ZipEntryProcessor {
		private TarArchiveOutputStream tao;
		private FilePredicate filePredicate;
		private String basePath;
		private int mode;

		TarArchivingZipEntryProcessor(TarArchiveOutputStream tao, FilePredicate filePredicate, String basePath, int mode) {
			this.tao = tao;
			this.filePredicate = filePredicate;
			this.basePath = basePath;
			this.mode = mode;
		}

		@Override
		public void process(ZipFile file, ZipEntry entry) throws IOException {
			try (InputStream inputStream = file.getInputStream(entry)) {
				if (filePredicate.evaluate(entry)) {
					addInputStreamToTar(this.tao, inputStream, basePath + FilenameUtils.getName(entry.getName()),
							entry.getSize(),
							this.mode);
				}
			}
		}
	}

}
