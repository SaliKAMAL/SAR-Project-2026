package edu.polytech.multicast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Records deliveries by direct method calls. It is not an actor and it does not
 * send messages.
 */
public final class Verifier {

  private final Map<String, List<Delivery>> byActor = new LinkedHashMap<String, List<Delivery>>();
  private final Set<String> leftActors = new HashSet<>();
  private final Map<String, Set<Long>> joinedAfter = new HashMap<>();
  private final Map<String, Set<Long>> leftAfter = new HashMap<>();
  private final Map<String, Set<Long>> missedMembership = new HashMap<>();

  public synchronized void delivered(String actorId, long messageId, byte[] payload) {
    if (leftActors.contains(actorId))
      throw new AssertionError(actorId + " delivered a message after leaving");
    List<Delivery> deliveries = byActor.get(actorId);
    if (deliveries == null) {
      deliveries = new ArrayList<Delivery>();
      byActor.put(actorId, deliveries);
    }
    deliveries.add(new Delivery(messageId, payload.clone()));
    notifyAll();
  }

  public synchronized void left(String actorId) {
    if (leftActors.add(actorId))
      leftAfter.put(actorId, deliveredIds());
    notifyAll();
  }

  public synchronized boolean hasLeft(String actorId) {
    return leftActors.contains(actorId);
  }

  public synchronized void joined(String actorId) {
    if (!joinedAfter.containsKey(actorId))
      joinedAfter.put(actorId, deliveredIds());
    notifyAll();
  }

  public synchronized boolean hasJoined(String actorId) {
    return joinedAfter.containsKey(actorId);
  }

  /** Keep messages missed during an absence out of later shared-delivery comparisons. */
  synchronized void resumed(String actorId) {
    Set<Long> missed = deliveredIds();
    List<Delivery> own = byActor.get(actorId);
    if (own != null)
      for (Delivery delivery : own)
        missed.remove(delivery.messageId);
    Set<Long> previous = missedMembership.get(actorId);
    if (previous == null)
      missedMembership.put(actorId, missed);
    else
      previous.addAll(missed);
  }

  private Set<Long> deliveredIds() {
    Set<Long> ids = new HashSet<>();
    for (List<Delivery> deliveries : byActor.values())
      for (Delivery delivery : deliveries)
        ids.add(delivery.messageId);
    return ids;
  }

  /** Compare only messages from the time both actors belonged to the group. */
  public synchronized void checkMembership(List<String> actorIds) {
    for (int i = 0; i < actorIds.size(); i++) {
      for (int j = i + 1; j < actorIds.size(); j++) {
        String first = actorIds.get(i);
        String second = actorIds.get(j);
        List<Delivery> a = sharedDeliveries(first, second);
        List<Delivery> b = sharedDeliveries(second, first);
        if (a.size() != b.size())
          throw new AssertionError("different delivery counts for " + first + " and " + second);
        Set<Long> seen = new HashSet<>();
        for (int k = 0; k < a.size(); k++) {
          Delivery expected = a.get(k);
          Delivery actual = b.get(k);
          if (!seen.add(expected.messageId) || expected.messageId != actual.messageId
              || !Arrays.equals(expected.payload, actual.payload))
            throw new AssertionError("shared delivery " + k + " differs for " + first + " and " + second);
        }
      }
    }
  }

  private List<Delivery> sharedDeliveries(String actorId, String peerId) {
    List<Delivery> result = new ArrayList<>();
    List<Delivery> deliveries = byActor.get(actorId);
    if (deliveries == null)
      return result;
    for (Delivery delivery : deliveries) {
      long id = delivery.messageId;
      if (joinedAfter.containsKey(actorId) && joinedAfter.get(actorId).contains(id))
        throw new AssertionError(actorId + " delivered a message from before joining");
      if (missedMembership.containsKey(actorId) && missedMembership.get(actorId).contains(id))
        throw new AssertionError(actorId + " delivered a message missed while absent");
      if (joinedAfter.containsKey(peerId) && joinedAfter.get(peerId).contains(id))
        continue;
      if (missedMembership.containsKey(peerId) && missedMembership.get(peerId).contains(id))
        continue;
      if ((leftAfter.containsKey(actorId) && !leftAfter.get(actorId).contains(id))
          || (leftAfter.containsKey(peerId) && !leftAfter.get(peerId).contains(id)))
        continue;
      result.add(delivery);
    }
    return result;
  }

  private final java.util.Set<String> crashedActors = new java.util.HashSet<String>();
  
  private final Map<String, Integer> crashReports = new HashMap<String, Integer>();

  public synchronized void crashed(String actorId) {
	    crashedActors.add(actorId);
	    Integer reports = crashReports.get(actorId);
	    crashReports.put(actorId, Integer.valueOf(reports == null ? 1 : reports.intValue() + 1));
	    notifyAll();
	  }
  
  public synchronized boolean awaitCrashed(String actorId, int reports, long timeoutMs)
	      throws InterruptedException {
	    long deadline = System.currentTimeMillis() + timeoutMs;
	    while (true) {
	      Integer seen = crashReports.get(actorId);
	      if (seen != null && seen.intValue() >= reports)
	        return true;
	      long remaining = deadline - System.currentTimeMillis();
	      if (remaining <= 0)
	        return false;
	      wait(remaining);
	    }
	  }

  public synchronized java.util.Set<String> getCrashedActors() {
    return crashedActors;
  }
  
  private volatile long lastDeliveryMsg = System.currentTimeMillis();
  private final Set<String> awaitingReinsertion = new HashSet<String>();
  private final Set<String> reinsertedActors = new HashSet<String>();
  
  /** Number of deliveries of the actor when its last life ended. */
  private final Map<String, Integer> lifeBoundary = new HashMap<String, Integer>();
  /** Ids delivered by the other actors before the actor came back: missed forever. */
  private final Map<String, Set<Long>> absentIds = new HashMap<String, Set<Long>>();
  
  public synchronized void resurrected(String actorId) {
	    List<Delivery> own = byActor.get(actorId);
	    lifeBoundary.put(actorId, Integer.valueOf(own == null ? 0 : own.size()));
	    Set<Long> absent = new HashSet<Long>();
	    for (Map.Entry<String, List<Delivery>> entry : byActor.entrySet()) {
	      if (entry.getKey().equals(actorId))
	        continue;
	      for (Delivery delivery : entry.getValue())
	        absent.add(Long.valueOf(delivery.messageId));
	    }
	    absentIds.put(actorId, absent);
	    awaitingReinsertion.add(actorId);
	    reinsertedActors.remove(actorId);
	    notifyAll();
	  }


  
  /** The actor is back in the view. */
  public synchronized void reinserted(String actorId) {
    if (!awaitingReinsertion.remove(actorId))
      throw new IllegalStateException(actorId + " was not resurrected");
    reinsertedActors.add(actorId);
    notifyAll();
  }
  
  /** Waits until reinserted(actorId) has been called for the current life. */
  public synchronized boolean awaitReinserted(String actorId, long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (!reinsertedActors.contains(actorId)) {
      long remaining = deadline - System.currentTimeMillis();
      if (remaining <= 0)
        return false;
      wait(remaining);
    }
    return true;
  }
  
  /** Waits until no actor has delivered anything for quietMs. False on timeout. */
  public synchronized boolean awaitQuiet(long quietMs, long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (true) {
      long now = System.currentTimeMillis();
      long quietFor = now - lastDeliveryMsg;
      if (quietFor >= quietMs)
        return true;
      long remaining = deadline - now;
      if (remaining <= 0)
        return false;
      wait(Math.min(remaining, quietMs - quietFor));
    }
  }
  public synchronized int deliveredCount(String actorId) {
	    List<Delivery> deliveries = byActor.get(actorId);
	    return deliveries == null ? 0 : deliveries.size();
	  }
  
  public synchronized List<String> deliveredPayloads(String actorId) {
	    List<String> result = new ArrayList<String>();
	    List<Delivery> deliveries = byActor.get(actorId);
	    if (deliveries != null)
	      for (Delivery delivery : deliveries)
	        result.add(new String(delivery.payload));
	    return result;
	  }
  
  /**
   * Checks the last life of a reinserted actor:
   * 1. it delivered something after its reinsertion,
   * 2. it never delivered an id missed while it was absent (missed forever),
   * 3. what it delivered is exactly the end of the sequence of the survivors.
   */
  public synchronized void checkReinserted(List<String> survivorIds, String reinsertedId) {
    if (!reinsertedActors.contains(reinsertedId))
      throw new AssertionError(reinsertedId + " was not reinserted");
    List<Delivery> all = byActor.get(reinsertedId);
    int from = lifeBoundary.get(reinsertedId).intValue();
    if (all == null || all.size() <= from)
      throw new AssertionError(reinsertedId + " delivered nothing after its reinsertion");
    List<Delivery> life = all.subList(from, all.size());

    Set<Long> absent = absentIds.get(reinsertedId);
    Set<Long> seen = new HashSet<Long>();
    for (Delivery delivery : life) {
      if (absent.contains(Long.valueOf(delivery.messageId)))
        throw new AssertionError(reinsertedId + " delivered message " + Long.toHexString(delivery.messageId)
            + " that it missed while it was absent");
      if (!seen.add(Long.valueOf(delivery.messageId)))
        throw new AssertionError(reinsertedId + " delivered message " + Long.toHexString(delivery.messageId)
            + " twice");
    }

    List<Delivery> reference = null;
    for (String survivorId : survivorIds) {
      reference = byActor.get(survivorId);
      if (reference != null)
        break;
    }
    if (reference == null)
      throw new AssertionError("no survivor delivered anything");
    if (life.size() > reference.size())
      throw new AssertionError(reinsertedId + " delivered more than the survivors after its reinsertion");
    int offset = reference.size() - life.size();
    for (int i = 0; i < life.size(); i++) {
      Delivery expected = reference.get(offset + i);
      Delivery actual = life.get(i);
      if (expected.messageId != actual.messageId || !Arrays.equals(expected.payload, actual.payload))
        throw new AssertionError(reinsertedId + " is not in the same order as the survivors at its delivery "
            + i + " after reinsertion");
    }
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
      if (deliveries == null) {
          deliveries = new java.util.ArrayList<Delivery>(); 
      }

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
