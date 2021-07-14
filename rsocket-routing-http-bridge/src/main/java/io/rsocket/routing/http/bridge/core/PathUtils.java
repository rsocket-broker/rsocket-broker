package io.rsocket.routing.http.bridge.core;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.http.server.PathContainer;

/**
 * @author Olga Maciaszek-Sharma
 */
class PathUtils {

	private static final Log LOG = LogFactory.getLog(PathUtils.class);

	private static final Set<String> interactionModeSegments = new HashSet<>(Arrays
			.asList("rr", "rc", "rs", "ff"));

	private PathUtils() {
		throw new IllegalStateException("Must not instantiate utility class");
	}

	static String resolveAddress(URI uri) {
		PathContainer path = PathContainer.parsePath(uri.getRawPath());
		List<PathContainer.Element> elements = getElements(path);
		if (pathStartsWithInteractionMode(elements)) {
			return elements.get(1).value();
		}
		return elements.get(0).value();
	}

	static String resolveRoute(URI uri) {
		PathContainer path = PathContainer.parsePath(uri.getRawPath());
		List<PathContainer.Element> elements = getElements(path);
		if (pathStartsWithInteractionMode(elements)) {
			return elements.get(2).value();
		}
		return elements.get(1).value();
	}

	private static List<PathContainer.Element> getElements(PathContainer path) {
		List<PathContainer.Element> pathElements = path.elements()
				.stream().filter(element -> !element.value().equals("/"))
				.collect(Collectors.toList());
		validate(pathElements);
		return pathElements;
	}

	private static void validate(List<PathContainer.Element> pathElements) {
		if (pathElements.size() < 2
				|| pathElements
				.size() == 3 && !pathStartsWithInteractionMode(pathElements)
				|| pathElements.size() > 3) {
			if (LOG.isErrorEnabled()) {
				LOG.error("The path does not contain correct number of elements. InteractionMode is optional, while Address and Route segments mandatory.");
			}
			throw new IllegalArgumentException("The path should have [InteractionMode], Address and Route segments.");
		}
	}

	private static boolean pathStartsWithInteractionMode(List<PathContainer.Element> pathElements) {
		return interactionModeSegments.contains(pathElements.get(0).value());
	}
}
