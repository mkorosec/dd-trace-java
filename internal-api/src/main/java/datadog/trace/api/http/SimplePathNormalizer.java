package datadog.trace.api.http;

final class SimplePathNormalizer extends PathNormalizer {

  private final int skipFirstXSegments;

  public SimplePathNormalizer(int skipFirstXSegments) {
    this.skipFirstXSegments = skipFirstXSegments;
  }

  public SimplePathNormalizer() {
    this(0);
  }

  @Override
  public String normalize(String path, boolean encoded) {
    if (null == path || path.isEmpty()) {
      return "/";
    }
    StringBuilder sb = new StringBuilder();
    int inEncoding = 0;
    int segmentsToSkip = skipFirstXSegments;
    for (int i = 0; i < path.length(); ) {
      int nextSlash = path.indexOf('/', i);
      if (nextSlash != i) {
        int endOfSegment = nextSlash == -1 ? path.length() : nextSlash;
        // detect version identifiers
        int segmentLength = (endOfSegment - i);
        if (segmentsToSkip-- > 0) {
          // skip this segment
          sb.append('?');
        } else if ((segmentLength <= 3 && segmentLength > 1 && (path.charAt(i) | ' ') == 'v')) {
          boolean numeric = true;
          for (int j = i + 1; j < endOfSegment; ++j) {
            numeric &= Character.isDigit(path.charAt(j));
          }
          if (numeric) {
            sb.append(path, i, endOfSegment);
          } else {
            sb.append('?');
          }
        } else {
          int snapshot = sb.length();
          boolean numeric = false;
          for (int j = i; j < endOfSegment && !numeric; ++j) {
            final char c = path.charAt(j);
            if (encoded && c == '%') {
              inEncoding = 3;
            }
            inEncoding--;
            numeric = inEncoding < 0 && Character.isDigit(c);
            if (!numeric && !Character.isWhitespace(c)) {
              sb.append(c);
            }
          }
          if (numeric) {
            sb.setLength(snapshot);
            sb.append('?');
          }
        }
        i = endOfSegment + 1;
      } else {
        ++i;
      }
      if (nextSlash != -1) {
        sb.append('/');
      }
    }
    return sb.length() == 0 ? "/" : sb.toString();
  }
}
