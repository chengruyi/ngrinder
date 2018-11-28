package org.ngrinder.agent.model;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.ngrinder.agent.service.AgentPackageService;
import org.ngrinder.infra.config.Config;
import org.ngrinder.infra.schedule.ScheduledTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.InputStream;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.commons.lang.StringUtils.isNotEmpty;
import static org.apache.commons.lang.StringUtils.trimToEmpty;
import static org.ngrinder.common.util.CollectionUtils.buildMap;
import static org.ngrinder.common.util.CollectionUtils.newHashMap;

/**
 * This enum is designed to support create packages.(agent, monitor)
 *
 * @since 3.5
 */
public enum NGrinderPackage {

	AGENT(
		"ngrinder-agent",
		"ngrinder-agent/",
		"ngrinder-agent/lib/",
		"classpath*:ngrinder-sh/agent/*",
		"agent_agent.conf",
		"dependencies.txt"
	),

	MONITOR(
		"ngrinder-monitor",
		"ngrinder-monitor/",
		"ngrinder-monitor/lib/",
		"classpath*:ngrinder-sh/monitor/*",
		"agent_monitor.conf",
		"monitor-dependencies.txt"
	);

	public static final int TIME_MILLIS_OF_DAY = 1000 * 60 * 60 * 24;
	private static Config config;
	private Logger LOGGER = LoggerFactory.getLogger(Package.class);

	private String moduleName;
	private String basePath;
	private String libPath;
	private String shellScriptsPath;
	private String templateName;
	private String dependenciesFileName;

	NGrinderPackage(String moduleName, String basePath, String libPath, String shellScriptsPath, String templateName, String dependenciesFileName) {
		this.moduleName = moduleName;
		this.basePath = basePath;
		this.libPath = libPath;
		this.shellScriptsPath = shellScriptsPath;
		this.templateName = templateName;
		this.dependenciesFileName = dependenciesFileName;
	}

	public String getModuleName() { return moduleName; }
	public String getBasePath() { return basePath; }
	public String getLibPath() { return libPath; }
	public String getShellScriptsPath() { return shellScriptsPath; }
	public String getTemplateName() { return templateName; }
	public String getDependenciesFileName() { return dependenciesFileName; }

	public Map<String, Object> getConfigParam(String regionName, String controllerIP, int port, String owner) {
		switch (this) {
			case AGENT:
				return getAgentConfigParam(regionName, controllerIP, port, owner);
			case MONITOR:
				return buildMap("monitorPort", (Object) String.valueOf(port));
			default:
				throw new IllegalArgumentException(this.getModuleName() + " module is not supported");
		}
	}

	public Set<String> getPackageDependentLibs(URLClassLoader urlClassLoader) {
		Set<String> libs = getDependentLibs(urlClassLoader);
		if (this.equals(AGENT)) {
			libs.add("ngrinder-core");
			libs.add("ngrinder-runtime");
			libs.add("ngrinder-groovy");
		}
		return libs;
	}

	public File makePackageTar(String regionName, String connectionIP, String ownerName, boolean forWindow) {
		File packageDir = getPackagesDir();
		if (packageDir.mkdirs()) {
			LOGGER.info("{} is created", packageDir.getPath());
		}
		final String packageName = getDistributionPackageName(regionName, connectionIP, ownerName, forWindow);
		return new File(packageDir, packageName);
	}

	/**
	 * Get the agent package containing folder.
	 *
	 * @return File  agent package dir.
	 */
	public static File getPackagesDir() {
		return config.getHome().getSubFile("download");
	}

	/**
	 * Get distributable package name with appropriate extension.
	 *
	 * @param regionName   region   namee
	 * @param connectionIP where it will connect to
	 * @param ownerName    owner name
	 * @param forWindow    if true, then package type is zip,if false, package type is tar.
	 * @return String  module full name.
	 */
	private String getDistributionPackageName(String regionName, String connectionIP, String ownerName, boolean forWindow) {
		return getPackageName() + getFilenameComponent(regionName) + getFilenameComponent(connectionIP) +
			getFilenameComponent(ownerName) + (forWindow ? ".zip" : ".tar");
	}

	/**
	 * Get package name
	 *
	 * @return String module full name.
	 */
	public String getPackageName() {
		return this.getModuleName() + "-" + config.getVersion();
	}

	public static void cleanUpPackageDir(boolean all) {
		synchronized (AgentPackageService.class) {
			final File packagesDir = NGrinderPackage.getPackagesDir();
			final File[] files = packagesDir.listFiles();
			if (files != null) {
				for (File each : files) {
					if (!each.isDirectory()) {
						long expiryTimestamp = each.lastModified() + (TIME_MILLIS_OF_DAY * 2);
						if (all || expiryTimestamp < System.currentTimeMillis()) {
							FileUtils.deleteQuietly(each);
						}
					}
				}
			}
		}
	}

	private String getFilenameComponent(String value) {
		value = trimToEmpty(value);
		if (isNotEmpty(value)) {
			value = "-" + value;
		}
		return value;
	}

	private Set<String> getDependentLibs(URLClassLoader urlClassLoader) {
		Set<String> libs = new HashSet<>();
		try (InputStream dependencyStream = urlClassLoader.getResourceAsStream(this.getDependenciesFileName())) {
			final String dependencies = IOUtils.toString(dependencyStream);
			for (String each : StringUtils.split(dependencies, ";")) {
				libs.add(each.trim().replace("-SNAPSHOT", ""));
			}
		} catch (Exception e) {
			LOGGER.error("Error while loading " + this.getDependenciesFileName(), e);
		}
		return libs;
	}

	private Map<String, Object> getAgentConfigParam(String regionName, String controllerIP, int port, String owner) {
		Map<String, Object> confMap = newHashMap();
		confMap.put("controllerIP", controllerIP);
		confMap.put("controllerPort", String.valueOf(port));
		if (StringUtils.isEmpty(regionName)) {
			regionName = "NONE";
		}
		if (StringUtils.isNotBlank(owner)) {
			if (StringUtils.isEmpty(regionName)) {
				regionName = "owned_" + owner;
			} else {
				regionName = regionName + "_owned_" + owner;
			}
		}
		confMap.put("controllerRegion", regionName);
		return confMap;
	}

	@Component
	public static class NGrinderPackageConfigBeanInjector {
		@Autowired
		private Config config;

		@Autowired
		private ScheduledTaskService scheduledTaskService;

		@PostConstruct
		public void postConstruct() {
			NGrinderPackage.config = config;
			// clean up package directories not to occupy too much spaces.
			NGrinderPackage.cleanUpPackageDir(true);

			scheduledTaskService.addFixedDelayedScheduledTask(new Runnable() {
				@Override
				public void run() {
					NGrinderPackage.cleanUpPackageDir(false);
				}
			}, TIME_MILLIS_OF_DAY);
		}
	}
}
