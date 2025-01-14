package datadog.trace.api.http;

import datadog.trace.api.Function;
import datadog.trace.api.cache.DDCache;
import datadog.trace.api.cache.DDCaches;
import datadog.trace.bootstrap.instrumentation.api.URIUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AntPatternPathNormalizer extends PathNormalizer {
  private static final Logger log = LoggerFactory.getLogger(AntPatternPathNormalizer.class);

  private final Map<String, String> resourceNameMatchers;
  private final PathNormalizer fallback;
  private final AntPathMatcher matcher = new AntPathMatcher();

  private final DDCache<String, String> cache = DDCaches.newFixedSizeCache(512);
  private final Function<String, String> cacheLoader =
      new Function<String, String>() {
        @Override
        public String apply(String path) {
          for (Map.Entry<String, String> resourceNameMatcher : resourceNameMatchers.entrySet()) {
            if (matcher.match(resourceNameMatcher.getKey(), path)) {
              return resourceNameMatcher.getValue();
            }
          }
          return fallback.normalize(path);
        }
      };

  AntPatternPathNormalizer(Map<String, String> httpResourceNameMatchers, PathNormalizer fallback) {
    resourceNameMatchers = httpResourceNameMatchers;
    this.fallback = fallback;

    // Clean up invalid patterns
    List<String> invalidPatterns = new ArrayList<>(httpResourceNameMatchers.keySet().size());
    for (String pattern : resourceNameMatchers.keySet()) {
      if (!matcher.isPattern(pattern)) {
        invalidPatterns.add(pattern);
      }
    }
    for (String invalid : invalidPatterns) {
      log.warn("Invalid pattern {} removed from matchers", invalid);
      resourceNameMatchers.remove(invalid);
    }
  }

  @Override
  public String normalize(String path, boolean encoded) {
    if (encoded) {
      path = URIUtils.decode(path);
    }
    return cache.computeIfAbsent(path, cacheLoader);
  }
}
