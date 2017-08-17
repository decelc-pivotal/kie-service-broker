package io.pivotal.kieservicebroker.deployer;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.cloudfoundry.operations.applications.ApplicationDetail;
import org.cloudfoundry.operations.applications.InstanceDetail;

public class CloudFoundryAppInstanceStatus implements AppInstanceStatus {

	private final InstanceDetail instanceDetail;

	private final ApplicationDetail applicationDetail;

	private final int index;

	private final Map<String, String> attributes = new TreeMap<>();

	public CloudFoundryAppInstanceStatus(ApplicationDetail applicationDetail, InstanceDetail instanceDetail,
			int index) {
		this.applicationDetail = applicationDetail;
		this.instanceDetail = instanceDetail;
		this.index = index;
	}

	@Override
	public String getId() {
		return applicationDetail.getName() + "-" + index;
	}

	@Override
	public DeploymentState getState() {
		if (instanceDetail == null) {
			return DeploymentState.failed;
		}
		switch (instanceDetail.getState()) {
		case "STARTING":
		case "DOWN":
			return DeploymentState.deploying;
		case "CRASHED":
			return DeploymentState.failed;
		// Seems the client incorrectly reports apps as FLAPPING when they are
		// obviously fine. Mapping as RUNNING for now
		case "FLAPPING":
		case "RUNNING":
			return DeploymentState.deployed;
		case "UNKNOWN":
			return DeploymentState.unknown;
		default:
			throw new IllegalStateException("Unsupported CF state: " + instanceDetail.getState());
		}
	}

	@Override
	public Map<String, String> getAttributes() {
		if (instanceDetail != null) {
			if (instanceDetail.getCpu() != null) {
				attributes.put("metrics.machine.cpu", String.format("%.1f%%", instanceDetail.getCpu() * 100d));
			}
			if (instanceDetail.getDiskQuota() != null && instanceDetail.getDiskUsage() != null) {
				attributes.put("metrics.machine.disk",
						String.format("%.1f%%", 100d * instanceDetail.getDiskUsage() / instanceDetail.getDiskQuota()));
			}
			if (instanceDetail.getMemoryQuota() != null && instanceDetail.getMemoryUsage() != null) {
				attributes.put("metrics.machine.memory", String.format("%.1f%%",
						100d * instanceDetail.getMemoryUsage() / instanceDetail.getMemoryQuota()));
			}
		}
		List<String> urls = applicationDetail.getUrls();
		if (!urls.isEmpty()) {
			attributes.put("url", "http://" + urls.get(0));
			for (int i = 0; i < urls.size(); i++) {
				attributes.put("url." + i, "http://" + urls.get(i));
			}
		}
		// TODO cf-java-client versions > 2.8 will have an index formally added
		// ot InstanceDetail
		attributes.put("guid", applicationDetail.getName() + ":" + index);
		return attributes;
	}

	@Override
	public String toString() {
		return String.format("%s[%s : %s]", getClass().getSimpleName(), getId(), getState());
	}
}