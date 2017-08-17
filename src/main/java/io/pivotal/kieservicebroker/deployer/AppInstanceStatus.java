package io.pivotal.kieservicebroker.deployer;

import java.util.Map;

public interface AppInstanceStatus {

	/**
	 * Return a unique identifier for the deployed app.
	 *
	 * @return identifier for the deployed app
	 */
	String getId();

	/**
	 * Return the state of the deployed app instance.
	 *
	 * @return state of the deployed app instance
	 */
	DeploymentState getState();

	/**
	 * Return a map of attributes for the deployed app instance. The specific
	 * keys/values returned are dependent on the runtime executing the app. This
	 * may include extra information such as deployment location or specific
	 * error messages in the case of failure.
	 *
	 * @return map of attributes for the deployed app
	 */
	Map<String, String> getAttributes();

}
