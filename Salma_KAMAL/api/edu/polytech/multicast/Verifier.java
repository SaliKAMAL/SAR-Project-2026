package edu.polytech.multicast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Records deliveries by direct method calls. It is not an actor and it does not
 * send messages.
 */
public final class Verifier {

  private final Map<String, List<Delivery>> byActor = new LinkedHashMap<String, List<Delivery>>();

  public synchronized void delivered(String actorId, long messageId, byte[] payload) {
    List<Delivery> deliveries = byActor.get(actorId);
    if (deliveries == null) {
      deliveries = new ArrayList<Delivery>();
      byActor.put(actorId, deliveries);
    }
    deliveries.add(new Delivery(messageId, payload.clone()));
    notifyAll();
  }

  private final java.util.Set<String> crashedActors = new java.util.HashSet<String>();

  public synchronized void crashed(String actorId) {
    crashedActors.add(actorId);
  }

  public synchronized java.util.Set<String> getCrashedActors() {
    return crashedActors;
  }
  
  public synchronized boolean awaitComplete(int actorCount, int messagesPerActor, long timeoutMs)
      throws InterruptedException {
    int expected = actorCount * messagesPerActor;
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (!complete(actorCount, expected)) {
      long remaining = deadline - System.currentTimeMillis();
      if (remaining <= 0)
        return false;
      wait(remaining);
    }
    return true;
  }

  public synchronized void check() {
    if (byActor.isEmpty())
      throw new AssertionError("no delivery was recorded");
    List<Delivery> reference = null;
    String referenceActor = null;
    for (Map.Entry<String, List<Delivery>> entry : byActor.entrySet()) {
      if (reference == null) {
        reference = entry.getValue();
        referenceActor = entry.getKey();
        continue;
      }
      List<Delivery> other = entry.getValue();
      if (other.size() != reference.size())
        throw new AssertionError(entry.getKey() + " delivered " + other.size()
            + " messages, " + referenceActor + " delivered " + reference.size());
      for (int i = 0; i < reference.size(); i++) {
        Delivery expected = reference.get(i);
        Delivery actual = other.get(i);
        if (expected.messageId != actual.messageId || !Arrays.equals(expected.payload, actual.payload))
          throw new AssertionError("delivery " + i + " differs between " + referenceActor + " and "
              + entry.getKey());
      }
    }
  }

  public synchronized void checkSurvivors(List<String> survivorIds) {
	if (byActor.isEmpty())
	  throw new AssertionError("no delivery was recorded");
	      
	List<Delivery> reference = null;
    String referenceActor = null;
	    
    for (String actorId : survivorIds) {
      List<Delivery> deliveries = byActor.get(actorId);
      if (deliveries == null) continue;

      if (reference == null) {
        reference = deliveries;
        referenceActor = actorId;
        continue;
      }
	      
      if (deliveries.size() != reference.size())
        throw new AssertionError(actorId + " delivered " + deliveries.size()
            + " messages, " + referenceActor + " delivered " + reference.size());
	      
      for (int i = 0; i < reference.size(); i++) {
        Delivery expected = reference.get(i);
        Delivery actual = deliveries.get(i);
        if (expected.messageId != actual.messageId || !java.util.Arrays.equals(expected.payload, actual.payload))
          throw new AssertionError("delivery " + i + " differs between " + referenceActor + " and " + actorId);
      }
    }
  }
  
  private boolean complete(int actorCount, int expected) {
    if (byActor.size() < actorCount)
      return false;
    for (List<Delivery> deliveries : byActor.values()) {
      if (deliveries.size() < expected)
        return false;
    }
    return true;
  }

  private static final class Delivery {
    final long messageId;
    final byte[] payload;

    Delivery(long messageId, byte[] payload) {
      this.messageId = messageId;
      this.payload = payload;
    }
  }
}
