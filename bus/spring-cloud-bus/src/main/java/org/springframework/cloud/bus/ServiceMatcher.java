package org.springframework.cloud.bus;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.PathMatcher;

/**
 * @author Spencer Gibb
 */
public class ServiceMatcher implements ApplicationContextAware {
	private ApplicationContext context;
	private PathMatcher matcher;

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		this.context = context;
	}

	@Autowired
	@Qualifier("busPathMatcher")
	public void setMatcher(PathMatcher matcher) {
		this.matcher = matcher;
	}

	public boolean isFromSelf(RemoteApplicationEvent event) {
		String originService = event.getOriginService();
		String serviceId = getServiceId();
		return matcher.match(originService, serviceId);
	}

	public boolean isForSelf(RemoteApplicationEvent event) {
		String destinationService = event.getDestinationService();
		return (destinationService == null || destinationService.trim().isEmpty() || matcher
				.match(destinationService, getServiceId()));
	}

	private String getServiceId() {
		return context.getId();
	}
}
