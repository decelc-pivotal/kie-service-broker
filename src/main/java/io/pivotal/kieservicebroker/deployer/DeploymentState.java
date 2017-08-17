package io.pivotal.kieservicebroker.deployer;

/**
 * Deployment states for apps and groups. These may represent the state of:
 * <ul>
 * <li>an entire group of apps</li>
 * <li>the global state of a deployed app as part of a group</li>
 * <li>the state of a particular instance of an app, in cases where
 * {@code app.count > 1}</li>
 * </ul>
 *
 */
public enum DeploymentState {

	/**
	 * The app or group is being deployed. If there are multiple apps or app
	 * instances, at least one of them is still being deployed.
	 */
	deploying,

	/**
	 * All apps have been successfully deployed.
	 */
	deployed,

	/**
	 * The app or group is known to the system, but is not currently deployed.
	 */
	undeployed,

	/**
	 * In the case of multiple apps, some have successfully deployed, while
	 * others have not. This state does not apply for individual app instances.
	 */
	partial,

	/**
	 * All apps have failed deployment.
	 */
	failed,

	/**
	 * A system error occurred trying to determine deployment status.
	 */
	error,

	/**
	 * The app or group deployment is not known to the system.
	 */
	unknown;

}
