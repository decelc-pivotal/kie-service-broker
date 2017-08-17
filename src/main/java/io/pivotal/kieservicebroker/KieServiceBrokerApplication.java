package io.pivotal.kieservicebroker;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.cloudfoundry.AbstractCloudFoundryException;
import org.cloudfoundry.UnknownCloudFoundryException;
import org.cloudfoundry.client.CloudFoundryClient;
import org.cloudfoundry.client.v2.applications.UpdateApplicationRequest;
import org.cloudfoundry.client.v2.applications.UpdateApplicationResponse;
//import org.cloudfoundry.client.v2.applications.StartApplicationRequest;
import org.cloudfoundry.doppler.DopplerClient;
import org.cloudfoundry.operations.CloudFoundryOperations;
import org.cloudfoundry.operations.DefaultCloudFoundryOperations;
import org.cloudfoundry.operations.applications.ApplicationDetail;
import org.cloudfoundry.operations.applications.ApplicationHealthCheck;
import org.cloudfoundry.operations.applications.ApplicationManifest;
import org.cloudfoundry.operations.applications.ApplicationSummary;
import org.cloudfoundry.operations.applications.GetApplicationRequest;
import org.cloudfoundry.operations.applications.PushApplicationManifestRequest;
import org.cloudfoundry.operations.applications.PushApplicationRequest;
import org.cloudfoundry.operations.applications.StartApplicationRequest;
import org.cloudfoundry.operations.buildpacks.Buildpack;
import org.cloudfoundry.operations.organizations.OrganizationSummary;
import org.cloudfoundry.reactor.ConnectionContext;
import org.cloudfoundry.reactor.DefaultConnectionContext;
import org.cloudfoundry.reactor.TokenProvider;
import org.cloudfoundry.reactor.client.ReactorCloudFoundryClient;
import org.cloudfoundry.reactor.doppler.ReactorDopplerClient;
import org.cloudfoundry.reactor.tokenprovider.PasswordGrantTokenProvider;
import org.cloudfoundry.reactor.uaa.ReactorUaaClient;
import org.cloudfoundry.uaa.UaaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
//import org.springframework.boot.autoconfigure.kafka.KafkaProperties.Consumer;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Hooks.OperatorHook;
import reactor.core.publisher.Mono;

@RestController
@SpringBootApplication
public class KieServiceBrokerApplication {

	private final String ORGANIZATION = "kie-server-org";
	private final String SPACE = "kie-server-space";
	private final String APP_SUFFIX = "-kie";

	Duration stagingTimeout = Duration.ofMillis(280);
	Duration startupTimeout = Duration.ofMillis(280);

	private static final Logger logger = LoggerFactory.getLogger(KieServiceBrokerApplication.class);

	@Autowired
	private ResourceLoader resourceLoader;

	@Bean
	DefaultConnectionContext connectionContext(@Value("${cf.apiHost}") String apiHost,
			@Value("${cf.skipSslValidation:true}") Boolean skipSslValidation) {
		return DefaultConnectionContext.builder().apiHost(apiHost).skipSslValidation(skipSslValidation).build();
	}

	@Bean
	PasswordGrantTokenProvider tokenProvider(@Value("${cf.username}") String username,
			@Value("${cf.password}") String password) {
		return PasswordGrantTokenProvider.builder().password(password).username(username).build();
	}

	@Bean
	ReactorDopplerClient dopplerClient(ConnectionContext connectionContext, TokenProvider tokenProvider) {
		return ReactorDopplerClient.builder().connectionContext(connectionContext).tokenProvider(tokenProvider).build();
	}

	@Bean
	ReactorUaaClient uaaClient(ConnectionContext connectionContext, TokenProvider tokenProvider) {
		return ReactorUaaClient.builder().connectionContext(connectionContext).tokenProvider(tokenProvider).build();
	}

	@Bean
	DefaultCloudFoundryOperations cloudFoundryOperations(CloudFoundryClient cloudFoundryClient,
			DopplerClient dopplerClient, UaaClient uaaClient, @Value("${cf.organization}") String organization,
			@Value("${cf.space}") String space) {
		return DefaultCloudFoundryOperations.builder().cloudFoundryClient(cloudFoundryClient)
				.dopplerClient(dopplerClient).uaaClient(uaaClient).organization(organization).space(space).build();
	}

	@Bean
	ReactorCloudFoundryClient cloudFoundryClient(ConnectionContext connectionContext, TokenProvider tokenProvider) {
		return ReactorCloudFoundryClient.builder().connectionContext(connectionContext).tokenProvider(tokenProvider)
				.build();
	}

	@Autowired
	private CloudFoundryOperations cloudFoundryOperations;

	@Autowired
	private CloudFoundryClient cloudFoundryClient;

	public static void main(String[] args) {

		Hooks.onOperator(providedHook -> providedHook.operatorStacktrace());
		SpringApplication.run(KieServiceBrokerApplication.class, args);
	}

	@GetMapping("/state")
	public @ResponseBody String getServerState() {

		String t = "<kie-server-state>" + "<controllers/>" + "<configuration>" + "<configItems>" + "<config-item>"
				+ "<name>org.kie.server.persistence.ds</name>" + "<value>java:comp/env/jdbc/jbpm</value>"
				+ "<type>java.lang.String</type>" + "</config-item>" + "<config-item>"
				+ "<name>org.kie.server.repo</name>" + "<value>/home/vcap/app/.java-buildpack/tomcat</value>"
				+ "<type>java.lang.String</type>" + "</config-item>" + "<config-item>"
				+ "<name>org.kie.server.persistence.tm</name>"
				+ "<value>org.hibernate.service.jta.platform.internal.BitronixJtaPlatform</value>"
				+ "<type>java.lang.String</type>" + "</config-item>" + "<config-item>"
				+ "<name>org.kie.server.id</name>" + "<value>cjdkieserver</value>" + "<type>java.lang.String</type>"
				+ "</config-item>" + "<config-item>" + "<name>cjd.id</name>" + "<value>cjdcjdcjdkieserver</value>"
				+ "<type>java.lang.String</type>" + "</config-item>" + "</configItems>" + "</configuration>"
				+ "<containers/>" + "</kie-server-state>";

		return t;
	}

	@GetMapping("/client")
	public @ResponseBody String getClient() {
		String user = "";
		String password = "";

		// Hooks.onOperator(providedHook -> providedHook.operatorStacktrace());

		Function<? super OperatorHook, ? extends OperatorHook> g = (x) -> {

			// System.out.println("get message=" + x.getMessage());
			// y.toString()
			return null;
		};

		// Hooks.onOperator(g);

		BiFunction<? super Throwable, Object, ? extends Throwable> f = (x, y) -> {

			System.out.println("getClient get message=" + x.getMessage());
			// y.toString()
			return new Throwable("getClient cjd");
		};

		Hooks.onOperatorError(f);

		// CloudCredentials credentials = new CloudCredentials(user, password);
		// CloudFoundryClient client = new ReactorCloudFoundryClient

		String t = "success";

		// cloudFoundryOperations.

		cloudFoundryOperations.organizations().list().map(OrganizationSummary::getName).subscribe(System.out::println);
		cloudFoundryOperations.buildpacks().list().map(Buildpack::getName).subscribe(System.out::println);
		cloudFoundryOperations.applications().list().map(ApplicationSummary::getName).subscribe(System.out::println);

		// pushApp();

		pushAppNew();

		return t;
	}

	private Mono<Void> pushAppNew() {

		BiFunction<? super Throwable, Object, ? extends Throwable> f = (x, y) -> {

			System.out.println("pushAppNew get message x=" + x.getStackTrace().length);

			int k = x.getStackTrace().length;
			StackTraceElement[] ll = x.getStackTrace();
			for (int i = 0; i < k; i++) {
				System.out.println("#### StackTraceElement=" + ll[i]);

			}

			// System.out.println("pushAppNew get message y=" + y.toString());
			// y.toString()
			return new Throwable("pushAppNew() cjd");
		};

		Hooks.onOperatorError(f);
		Hooks.onOperator(providedHook -> providedHook.operatorStacktrace());

		String applicationName = "kie-server-test-1";
		String jarResourcePath = "classpath:war-files/kie-server-6.5.0.Final-webc.war";

		Resource resource = resourceLoader.getResource(jarResourcePath);
		InputStream stream = null;
		try {
			// resource
			if (resource != null) {
				stream = resource.getInputStream();
				System.out.println("####### before push");

			}

			Mono<Void> x = pushApplication(resource.getFile().toPath(), applicationName, true)
					.timeout(Duration.ofSeconds(580)).doOnTerminate((item, error) -> {
						if (error == null) {
							logger.info("Successfully deployed {}", applicationName);
						} else if (isNotFoundError().test(error)) {
							logger.warn(
									"Unable to deploy application. It may have been destroyed before start completed: "
											+ error.getMessage());
						} else {
							logError(String.format("Failed to deploy %s", applicationName)).accept(error);
						}
					}).subscribe();

			return x;

		} catch (

		IOException e) {
			IllegalStateException ex = new IllegalStateException(
					"Error getting application archive from resource " + jarResourcePath, e);
			System.out.println("####### after push t=" + ex.getMessage());
			throw ex;
		}

	}

	private Mono<Void> pushApplication(Path application, String name, Boolean noStart) {

		String jarResourcePath = "classpath:war-files/kie-server-6.5.0.Final-webc.war";

		Resource resource = resourceLoader.getResource(jarResourcePath);
		InputStream stream = null;
		try {
			// resource
			if (resource != null) {
				stream = resource.getInputStream();
				System.out.println("####### before push");

				ApplicationManifest.Builder manifest = ApplicationManifest.builder().path(application)
						.buildpack("https://github.com/decelc-pivotal/java-buildpack").disk(512)
						.environmentVariables(getEnvironmentVariables()).healthCheckType(ApplicationHealthCheck.PORT)
						.instances(1).memory(1024).name(name);

				return requestPushApplication(PushApplicationManifestRequest.builder().manifest(manifest.build())
						.stagingTimeout(stagingTimeout).startupTimeout(startupTimeout).build())
								.doOnSuccess(v -> logger.info("Done uploading bits for {}", name))
								.doOnError(e -> logger.error(String
										.format("Error creating app %s.  Exception Message %s", name, e.getMessage())));

				// System.out.println("####### after push t=");

			}

		} catch (

		IOException e) {
			IllegalStateException ex = new IllegalStateException(
					"Error getting application archive from resource " + jarResourcePath, e);
			System.out.println("####### after push t=" + ex.getMessage());
			throw ex;
		}

		return null;

		/*
		 * o =
		 * cloudFoundryOperations.applications().push(PushApplicationRequest.
		 * builder().path(application)
		 * .buildpack("https://github.com/decelc-pivotal/java-buildpack").
		 * timeout(180).diskQuota(512)
		 * .healthCheckType(ApplicationHealthCheck.PORT).memory(1024).name(name)
		 * .noStart(noStart).build()) .doOnSuccess(v ->
		 * logger.info("Done uploading bits for {}", "cjd")) .doOnError(e ->
		 * logger
		 * .error(String.format("Error creating app %s. Exception Message %s",
		 * "cjd", e.getMessage()))) .then(setEnvironmentVariables(name,
		 * getEnvironmentVariables()));// .then(startApplication(name));
		 * 
		 * // ;
		 */

		/*
		 * .path(getApplication(request)) // Only one of the two is non-null
		 * //.dockerImage(getDockerImage(request)) // Only one of the two is
		 * non-null .buildpack(buildpack(request)) .disk(diskQuota(request))
		 * .environmentVariables(getEnvironmentVariables(deploymentId, request))
		 * .healthCheckType(healthCheck(request)) .instances(instances(request))
		 * .memory(memory(request)) .name(deploymentId)
		 * .noRoute(toggleNoRoute(request)) .services(servicesToBind(request));
		 */

	}

	private static URL getTargetURL(String target) {
		try {
			return URI.create(target).toURL();
		} catch (MalformedURLException e) {
			throw new RuntimeException("The target URL is not valid: " + e.getMessage());
		}
	}

	private void pushApp() {
		String jarResourcePath = "classpath:war-files/kie-server-6.5.0.Final-webc.war";

		BiFunction<? super Throwable, Object, ? extends Throwable> f = (x, y) -> {

			System.out.println("pushApp get message=" + x.getMessage());
			// y.toString()
			return new Throwable("pushApp cjd");
		};

		Hooks.onOperatorError(f);
		Hooks.onOperator(providedHook -> providedHook.operatorStacktrace());

		try {
			Resource resource = resourceLoader.getResource(jarResourcePath);
			System.out.println("#### resource = " + resource);

			// resource
			if (resource != null) {
				InputStream stream = resource.getInputStream();
				System.out.println("####### before push");

				String applicationName = "kie-server-test-1";

				createApplication(this.cloudFoundryOperations, resource.getFile().toPath(), applicationName, false)

						.doOnTerminate((item, error) -> {
							if (error == null) {
								logger.info("Successfully deployed {}", "cjd");
							} else {
								logger.info("Failed deployed {}", "cjd");
							}
						}).subscribe();

				;

				System.out.println("####### after push t=");

			}

		} catch (IOException e) {
			IllegalStateException ex = new IllegalStateException(
					"Error getting application archive from resource " + jarResourcePath, e);
			System.out.println("####### after push t=" + ex.getMessage());
			throw ex;
		}
	}

	private Mono<UpdateApplicationResponse> createApplication(CloudFoundryOperations cloudFoundryOperations,
			Path application, String name, Boolean noStart) {

		Hooks.onOperator(providedHook -> providedHook.operatorStacktrace());

		BiFunction<? super Throwable, Object, ? extends Throwable> f = (x, y) -> {

			System.out.println("createApplication get message=" + x.getMessage());
			// y.toString()
			return new Throwable("createApplication");
		};

		Hooks.onOperatorError(f);

		Mono<UpdateApplicationResponse> o = null;

		try {
			o = cloudFoundryOperations.applications().push(PushApplicationRequest.builder().path(application)
					.buildpack("https://github.com/decelc-pivotal/java-buildpack").timeout(180).diskQuota(512)
					.healthCheckType(ApplicationHealthCheck.PORT).memory(1024).name(name).noStart(noStart).build())
					.doOnSuccess(v -> logger.info("Done uploading bits for {}", "cjd"))
					.doOnError(e -> logger
							.error(String.format("Error creating app %s. Exception Message %s", "cjd", e.getMessage())))
					.then(setEnvironmentVariables(name, getEnvironmentVariables()));// .then(startApplication(name));

			// ;

			System.out.println("##### test=" + o.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return o;
	}

	private static Flux<ApplicationSummary> requestListApplications(CloudFoundryOperations cloudFoundryOperations) {
		return cloudFoundryOperations.applications().list();
	}

	private String getCatalinaOpts() {

		String s = "-XX:MaxDirectMemorySize=1G -Dorg.kie.deployment.desc.location=file:/home/vcap/app/.java-buildpack/tomcat/conf/deployment-descriptor.xml -Dbtm.root=/home/vcap/app/.java-buildpack/tomcat -Dbitronix.tm.configuration=/home/vcap/app/.java-buildpack/tomcat/conf/btm-config.properties -Djbpm.tsr.jndi.lookup=java:comp/env/TransactionSynchronizationRegistry -Dorg.jboss.logging.provider=jdk -Djava.security.auth.login.config=/home/vcap/app/.java-buildpack/tomcat/webapps/ROOT/WEB-INF/classes/login.config  -Dorg.jbpm.server.ext.disabled=false -Dorg.drools.server.ext.disabled=false -Dorg.jbpm.cdi.bm=java:comp/env/BeanManager -Dorg.kie.server.persistence.ds=java:comp/env/jdbc/jbpm -Dorg.kie.server.persistence.tm=org.hibernate.service.jta.platform.internal.BitronixJtaPlatform -Dorg.kie.server.id=cjdkieserver -Dkie.maven.settings.custom=/home/vcap/app/.java-buildpack/tomcat/conf/settings.xml";
		return s;

	}

	private Mono<UpdateApplicationResponse> setEnvironmentVariables(String deploymentId,
			Map<String, String> environmentVariables) {
		return getApplicationId(deploymentId)
				.then(applicationId -> requestUpdateApplication(applicationId, environmentVariables))
				.doOnSuccess(v -> logger.debug("Setting individual env variables to {} for app {}",
						environmentVariables, deploymentId))
				.doOnError(e -> logger
						.error(String.format("Unable to set individual env variables for app %s", deploymentId)));
	}

	private Mono<String> getApplicationId(String deploymentId) {
		return requestGetApplication(deploymentId).map(ApplicationDetail::getId);
	}

	private Mono<ApplicationDetail> requestGetApplication(String id) {
		return this.cloudFoundryOperations.applications().get(GetApplicationRequest.builder().name(id).build());
	}

	private Mono<UpdateApplicationResponse> requestUpdateApplication(String applicationId,
			Map<String, String> environmentVariables) {
		return this.cloudFoundryClient.applicationsV2().update(UpdateApplicationRequest.builder()
				.applicationId(applicationId).environmentJsons(environmentVariables).build());
	}

	private Map<String, String> getEnvironmentVariables() {
		Map<String, String> envVariables = new HashMap<>();

		envVariables.put("CATALINA_OPTS", getCatalinaOpts());
		return envVariables;
	}

	private Mono<Void> startApplication(String deploymentId) {
		return requestStartApplication(deploymentId, stagingTimeout, startupTimeout).doOnTerminate((item, error) -> {
			if (error == null) {
				logger.info("Started app {}", deploymentId);
				// } else if (isNotFoundError().test(error)) {
				// logger.warn("Unable to start application. It may have been
				// destroyed before start completed: "
				// + error.getMessage());
				// } else {
				// logger.error(String.format("Failed to start app %s",
				// deploymentId)).accept(error);
			} else {
				logger.error("Error starting app {}", deploymentId);
			}
		});
	}

	private Mono<Void> requestStartApplication(String name, Duration stagingTimeout, Duration startupTimeout) {
		return cloudFoundryOperations.applications().start(StartApplicationRequest.builder().name(name)
				.stagingTimeout(stagingTimeout).startupTimeout(startupTimeout).build());
	}

	private Mono<Void> requestPushApplication(PushApplicationManifestRequest request) {

		System.out.println("######## PushApplicationManifestRequest =" + request.toString());
		return cloudFoundryOperations.applications().pushManifest(request);
	}

	/**
	 * Return a function usable in {@literal doOnError} constructs that will
	 * unwrap unrecognized Cloud Foundry Exceptions and log the text payload.
	 */
	protected Consumer<Throwable> logError(String msg) {
		return e -> {
			if (e instanceof UnknownCloudFoundryException) {
				logger.error(msg + "\nUnknownCloudFoundryException encountered, whose payload follows:\n"
						+ ((UnknownCloudFoundryException) e).getPayload(), e);
			} else {
				logger.error(msg, e);
			}
		};
	}

	Predicate<Throwable> isNotFoundError() {
		return t -> t instanceof AbstractCloudFoundryException
				&& ((AbstractCloudFoundryException) t).getStatusCode() == HttpStatus.NOT_FOUND.value();
	}
}
