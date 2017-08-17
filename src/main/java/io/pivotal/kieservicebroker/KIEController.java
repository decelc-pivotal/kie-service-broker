package io.pivotal.kieservicebroker;

import java.io.IOException;
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
import org.cloudfoundry.doppler.DopplerClient;
import org.cloudfoundry.operations.CloudFoundryOperations;
import org.cloudfoundry.operations.DefaultCloudFoundryOperations;
import org.cloudfoundry.operations.applications.ApplicationDetail;
import org.cloudfoundry.operations.applications.ApplicationHealthCheck;
import org.cloudfoundry.operations.applications.ApplicationManifest;
import org.cloudfoundry.operations.applications.ApplicationSummary;
import org.cloudfoundry.operations.applications.GetApplicationRequest;
import org.cloudfoundry.operations.applications.InstanceDetail;
import org.cloudfoundry.operations.applications.PushApplicationManifestRequest;
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
import org.cloudfoundry.util.DelayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.pivotal.kieservicebroker.deployer.AppStatus;
import io.pivotal.kieservicebroker.deployer.CloudFoundryAppInstanceStatus;
import io.pivotal.kieservicebroker.deployer.DeploymentState;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

@RestController
@SpringBootApplication
public class KIEController {

	private final String ORGANIZATION = "kie-server-org";
	private final String SPACE = "kie-server-space";
	private final String APP_SUFFIX = "-kie";

	Duration stagingTimeout = Duration.ofMinutes(3);
	Duration startupTimeout = Duration.ofMinutes(3);

	private static final Logger logger = LoggerFactory.getLogger(KIEController.class);

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

	public static void main(String[] args) {

		Hooks.onOperator(providedHook -> providedHook.operatorStacktrace());
		SpringApplication.run(KIEController.class, args);
	}

	@GetMapping("/newClient")
	public @ResponseBody String getClient() {
		String user = "";
		String password = "";

		Hooks.onOperator(providedHook -> providedHook.operatorStacktrace());

		// Hooks.onOperator(g);

		BiFunction<? super Throwable, Object, ? extends Throwable> f = (x, y) -> {

			System.out.println("getClient get message=" + x.getMessage());
			// y.toString()
			return new Throwable("getClient cjd");
		};

		Hooks.onOperatorError(f);
		Hooks.onOperator(providedHook -> providedHook.operatorStacktrace());

		// CloudCredentials credentials = new CloudCredentials(user, password);
		// CloudFoundryClient client = new ReactorCloudFoundryClient

		String t = "success";

		cloudFoundryOperations.organizations().list().map(OrganizationSummary::getName).subscribe(System.out::println);
		cloudFoundryOperations.buildpacks().list().map(Buildpack::getName).subscribe(System.out::println);
		cloudFoundryOperations.applications().list().map(ApplicationSummary::getName).subscribe(System.out::println);

		pushAppNew();

		return t;
	}

	private String pushAppNew() {

		String t = "te";

		BiFunction<? super Throwable, Object, ? extends Throwable> f = (x, y) -> {

			System.out.println("pushAppNew get message x=" + x.getStackTrace().length);

			int k = x.getStackTrace().length;
			StackTraceElement[] ll = x.getStackTrace();
			for (int i = 0; i < k; i++) {
				System.out.println("#### StackTraceElement=" + ll[i]);

			}

			return new Throwable("pushAppNew() cjd");
		};

		Hooks.onOperatorError(f);
		Hooks.onOperator(providedHook -> providedHook.operatorStacktrace());

		String applicationName = "kie-server-test-1";
		String jarResourcePath = "classpath:war-files/kie-server-6.5.0.Final-webc.war";

		Resource resource = resourceLoader.getResource(jarResourcePath);
		try {
			// resource
			if (resource != null) {
				System.out.println("####### before push");
			}

			/*
			 * getStatus(applicationName).doOnNext(status ->
			 * assertApplicationDoesNotExist(deploymentId, status)) // Need to
			 * block here to be able to throw exception early
			 * .block(Duration.ofSeconds(180));
			 * 
			 */

			logger.trace("deploy: Pushing application");
			pushApplication(resource.getFile().toPath(), applicationName, false).timeout(Duration.ofSeconds(180))
					.doOnTerminate((item, error) -> {
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

			logger.trace("Exiting deploy().  Deployment Id = {}", applicationName);

		} catch (

		IOException e) {
			IllegalStateException ex = new IllegalStateException(
					"Error getting application archive from resource " + jarResourcePath, e);
			System.out.println("####### after push t=" + ex.getMessage());
			throw ex;
		}

		return t;

	}

	private Mono<Void> pushApplication(Path application, String name, Boolean noStart) {

		ApplicationManifest.Builder manifest = ApplicationManifest.builder().path(application)
				.buildpack("https://github.com/decelc-pivotal/java-buildpack").disk(512)
				.environmentVariables(getEnvironmentVariables()).healthCheckType(ApplicationHealthCheck.PORT)
				.instances(1).memory(1024).name(name);

		PushApplicationManifestRequest t = PushApplicationManifestRequest.builder().manifest(manifest.build())
				.stagingTimeout(stagingTimeout).startupTimeout(startupTimeout).build();

		System.out.println("####### after push PushApplicationManifestRequest t=" + t);

		return requestPushApplication(t).doOnSuccess(v -> logger.info("Done uploading bits for {}", name)).doOnError(
				e -> logger.error(String.format("Error creating app %s.  Exception Message %s", name, e.getMessage())));

	}

	private String getCatalinaOpts() {

		String s = "-XX:MaxDirectMemorySize=1G -Dorg.kie.deployment.desc.location=file:/home/vcap/app/.java-buildpack/tomcat/conf/deployment-descriptor.xml -Dbtm.root=/home/vcap/app/.java-buildpack/tomcat -Dbitronix.tm.configuration=/home/vcap/app/.java-buildpack/tomcat/conf/btm-config.properties -Djbpm.tsr.jndi.lookup=java:comp/env/TransactionSynchronizationRegistry -Dorg.jboss.logging.provider=jdk -Djava.security.auth.login.config=/home/vcap/app/.java-buildpack/tomcat/webapps/ROOT/WEB-INF/classes/login.config  -Dorg.jbpm.server.ext.disabled=false -Dorg.drools.server.ext.disabled=false -Dorg.jbpm.cdi.bm=java:comp/env/BeanManager -Dorg.kie.server.persistence.ds=java:comp/env/jdbc/jbpm -Dorg.kie.server.persistence.tm=org.hibernate.service.jta.platform.internal.BitronixJtaPlatform -Dorg.kie.server.id=cjdkieserver -Dkie.maven.settings.custom=/home/vcap/app/.java-buildpack/tomcat/conf/settings.xml";
		return s;

	}

	private Map<String, String> getEnvironmentVariables() {
		Map<String, String> envVariables = new HashMap<>();

		envVariables.put("CATALINA_OPTS", getCatalinaOpts());
		return envVariables;
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

	private Mono<AppStatus> getStatus(String deploymentId) {
		return requestGetApplication(deploymentId)
				.map(applicationDetail -> createAppStatus(applicationDetail, deploymentId))
				.onErrorResume(IllegalArgumentException.class, t -> {
					logger.debug("Application for {} does not exist.", deploymentId);
					return Mono.just(createEmptyAppStatus(deploymentId));
				}).transform(statusRetry(deploymentId)).onErrorReturn(createErrorAppStatus(deploymentId));
	}

	private Mono<ApplicationDetail> requestGetApplication(String id) {
		return this.cloudFoundryOperations.applications().get(GetApplicationRequest.builder().name(id).build());
	}

	private AppStatus createAppStatus(ApplicationDetail applicationDetail, String deploymentId) {
		logger.trace("Gathering instances for " + applicationDetail);
		logger.trace("InstanceDetails: " + applicationDetail.getInstanceDetails());

		AppStatus.Builder builder = AppStatus.of(deploymentId);

		int i = 0;
		for (InstanceDetail instanceDetail : applicationDetail.getInstanceDetails()) {
			builder.with(new CloudFoundryAppInstanceStatus(applicationDetail, instanceDetail, i++));
		}
		for (; i < applicationDetail.getInstances(); i++) {
			builder.with(new CloudFoundryAppInstanceStatus(applicationDetail, null, i));
		}

		return builder.build();
	}

	private AppStatus createEmptyAppStatus(String deploymentId) {
		return AppStatus.of(deploymentId).build();
	}

	private AppStatus createErrorAppStatus(String deploymentId) {
		return AppStatus.of(deploymentId).generalState(DeploymentState.error).build();
	}

	/*
	 * private Mono<AppStatus> getStatus(String deploymentId) { return
	 * requestGetApplication(deploymentId) .map(applicationDetail ->
	 * createAppStatus(applicationDetail, deploymentId))
	 * .onErrorResume(IllegalArgumentException.class, t -> {
	 * logger.debug("Application for {} does not exist.", deploymentId); return
	 * Mono.just(createEmptyAppStatus(deploymentId));
	 * }).transform(statusRetry(deploymentId)).onErrorReturn(
	 * createErrorAppStatus(deploymentId)); }
	 */

	/**
	 * To be used in order to retry the status operation for an application or
	 * task.
	 * 
	 * @param id
	 *            The application id or the task id
	 * @param <T>
	 *            The type of status object being queried for, usually AppStatus
	 *            or TaskStatus
	 * @return The function that executes the retry logic around for determining
	 *         App or Task Status
	 */
	<T> Function<Mono<T>, Mono<T>> statusRetry(String id) {
		long statusTimeout = 520;
		long requestTimeout = Math.round(statusTimeout * 0.40); // wait 500ms
																// with default
																// status
																// timeout of
																// 2000ms
		long initialRetryDelay = Math.round(statusTimeout * 0.10); // wait 200ms
																	// with
																	// status
																	// timeout
																	// of 2000ms

		if (requestTimeout < 500L) {
			logger.info("Computed statusRetry Request timeout = {} ms is below 500ms minimum value.  Setting to 500ms",
					requestTimeout);
			requestTimeout = 500L;
		}
		final long requestTimeoutToUse = requestTimeout;
		return m -> m.timeout(Duration.ofMillis(requestTimeoutToUse)).doOnError(e -> logger.error(
				String.format("Error getting status for %s within %sms, Retrying operation.", id, requestTimeoutToUse)))
				.retryWhen(DelayUtils
						.exponentialBackOffError(Duration.ofMillis(initialRetryDelay), // initial
																						// retry
																						// delay
								Duration.ofMillis(statusTimeout / 2), // max
																		// retry
																		// delay
								Duration.ofMillis(statusTimeout)) // max total
																	// retry
																	// time
						.andThen(retries -> Flux.from(retries).doOnComplete(
								() -> logger.info("Successfully retried getStatus operation status [{}] for {}", id))))
				.doOnError(e -> logger.error(String.format(
						"Retry operation on getStatus failed for %s.  Max retry time %sms", id, statusTimeout)));
	}
}
