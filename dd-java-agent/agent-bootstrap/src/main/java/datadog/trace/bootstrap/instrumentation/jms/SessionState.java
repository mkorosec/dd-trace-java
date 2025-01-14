package datadog.trace.bootstrap.instrumentation.jms;

import datadog.trace.bootstrap.instrumentation.api.AgentScope;
import datadog.trace.bootstrap.instrumentation.api.AgentSpan;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Holds message spans consumed in client-acknowledged or transacted sessions. This class needs to
 * be thread-safe as some JMS providers allow concurrent transactions.
 */
public final class SessionState {

  private static final AtomicIntegerFieldUpdater<SessionState> SCOPE_COUNT =
      AtomicIntegerFieldUpdater.newUpdater(SessionState.class, "scopeCount");

  // hard bound at 8192 captured spans, degrade to finishing spans early
  // if transactions are very large, rather than use lots of space
  static final int MAX_CAPTURED_SPANS = 8192;

  private final int ackMode;

  // consumer-related session state
  private final Map<Thread, AgentScope> activeScopes = new ConcurrentHashMap<>();
  private final Deque<AgentSpan> capturedSpans = new ArrayDeque<>();
  private volatile int scopeCount = 0;

  // this field is protected by synchronization of capturedSpans, but SpotBugs miss that
  @SuppressFBWarnings("IS2_INCONSISTENT_SYNC")
  private boolean capturingFlipped = false;

  public SessionState(int ackMode) {
    this.ackMode = ackMode;
  }

  public boolean isTransactedSession() {
    return ackMode == 0; /* Session.SESSION_TRANSACTED */
  }

  public boolean isClientAcknowledge() {
    return ackMode == 2; /* Session.CLIENT_ACKNOWLEDGE */
  }

  public boolean isAutoAcknowledge() {
    return ackMode != 0 && ackMode != 2; /* treat all other modes as Session.AUTO_ACKNOWLEDGE */

    // We can't be sure of the ack-pattern for non-standard vendor modes, so the safest thing
    // to do is close+finish message spans on the next receive like we do for AUTO_ACKNOWLEDGE
  }

  // only used for testing
  int getCapturedSpanCount() {
    synchronized (capturedSpans) {
      return capturedSpans.size();
    }
  }

  /** Finishes the given message span when a message from the same session is acknowledged. */
  public void finishOnAcknowledge(AgentSpan span) {
    captureMessageSpan(span);
  }

  /** Finishes the given message span when the session is committed, rolled back, or closed. */
  public void finishOnCommit(AgentSpan span) {
    captureMessageSpan(span);
  }

  private void captureMessageSpan(AgentSpan span) {
    synchronized (capturedSpans) {
      if (capturedSpans.size() < MAX_CAPTURED_SPANS) {
        // change capture direction of the deque on each commit/ack
        // avoids mixing new spans with the old group while still supporting LIFO
        if (capturingFlipped) {
          capturedSpans.addFirst(span);
        } else {
          capturedSpans.addLast(span);
        }
        return;
      }
    }
    // just finish the span to avoid an unbounded queue
    span.finish();
  }

  public void onAcknowledge() {
    finishCapturedSpans();
  }

  public void onCommitOrRollback() {
    finishCapturedSpans();
  }

  private void finishCapturedSpans() {
    // make sure we finish spans in this commit/ack before any subsequent commit/ack
    synchronized (this) {
      int spansToFinish;
      boolean finishingFlipped;
      synchronized (capturedSpans) {
        spansToFinish = capturedSpans.size();
        // if capturing was flipped for this group then we need to flip finishing to match
        finishingFlipped = capturingFlipped;
        // update capturing to use the other end of the deque for the next group of spans
        capturingFlipped = !finishingFlipped;
      }
      for (int i = 0; i < spansToFinish; ++i) {
        AgentSpan span;
        synchronized (capturedSpans) {
          // finish spans in LIFO order according to how they were captured
          // for example addFirst --> pollFirst vs. addLast --> pollLast
          if (finishingFlipped) {
            span = capturedSpans.pollFirst();
          } else {
            span = capturedSpans.pollLast();
          }
        }
        // it won't be null, but just in case...
        if (null != span) {
          span.finish();
        }
      }
    }
  }

  /** Closes the given message scope when the next message is consumed or the session is closed. */
  void closeOnIteration(AgentScope newScope) {
    if (SCOPE_COUNT.incrementAndGet(this) > 100) {
      closeStaleScopes();
    }
    maybeCloseScope(activeScopes.put(Thread.currentThread(), newScope));
  }

  /** Closes the scope previously registered by closeOnIteration, assumes same calling thread. */
  void closePreviousMessageScope() {
    maybeCloseScope(activeScopes.remove(Thread.currentThread()));
  }

  /** Closes any active message scopes and finishes any pending transacted spans. */
  public void onClose() {
    for (AgentScope scope : activeScopes.values()) {
      maybeCloseScope(scope);
    }
    activeScopes.clear();
    if (!isAutoAcknowledge()) {
      finishCapturedSpans();
    }
  }

  private void maybeCloseScope(AgentScope scope) {
    if (null != scope) {
      SCOPE_COUNT.decrementAndGet(this);
      scope.close();
      if (isAutoAcknowledge()) {
        scope.span().finish();
      }
    }
  }

  private void closeStaleScopes() {
    Iterator<Map.Entry<Thread, AgentScope>> itr = activeScopes.entrySet().iterator();
    while (itr.hasNext()) {
      Map.Entry<Thread, AgentScope> entry = itr.next();
      if (!entry.getKey().isAlive()) {
        maybeCloseScope(entry.getValue());
        itr.remove();
      }
    }
  }
}
