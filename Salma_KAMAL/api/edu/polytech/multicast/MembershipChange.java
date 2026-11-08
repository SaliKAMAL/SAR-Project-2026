package edu.polytech.multicast;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.polytech.multicast.MembershipMessage.Type;

/** Replies and local progress for one join or leave. One actor owns each instance. */
public final class MembershipChange {
  public final MembershipRequest request;
  public final Map<String, Long> members;
  public final Map<String, Long> participants;
  public final Map<String, Long> nextMembers;
  final Set<String> markers = new HashSet<>();
  final Map<String, Long> clocks = new HashMap<>();
  private final Map<String, MembershipMessage> confirmations = new HashMap<>();
  final Map<String, MembershipMessage> statuses = new HashMap<>();
  private final Map<String, MembershipMessage> recovered = new HashMap<>();
  Map<String, Long> survivors = new HashMap<>();
  public long boundary = -1;
  public boolean applied;
  boolean notified;
  boolean recovering;
  boolean resolved;

  public MembershipChange(MembershipRequest request, Map<String, Long> members) {
    this.request = request;
    this.members = new HashMap<>(members);
    participants = new HashMap<>(members);
    nextMembers = new HashMap<>(members);
    
    if (request.operation() == MembershipRequest.Operation.JOIN) {
      participants.put(request.actorId(), request.generationId());
      nextMembers.put(request.actorId(), request.generationId());
    } else {
      nextMembers.remove(request.actorId());
    }
  }

  public boolean record(MembershipMessage message) {
    if (!request.equals(message.request) || !members.equals(message.members) || !Long.valueOf(message.generationId).equals(participants.get(message.senderId)))
      return false;
    if (message.type == Type.STATUS || (message.type == Type.CONFIRM && !message.survivors.isEmpty())) {
      if (!survivors.equals(message.survivors) || !survivors.containsKey(message.senderId))
        return false;
      
      Map<String, MembershipMessage> replies = message.type == Type.STATUS ? statuses : recovered;
      return replies.putIfAbsent(message.senderId, message) == null;
    }
    if (recovering || resolved)
      return false;
    switch (message.type) {
      case FLUSH:
        return members.containsKey(message.senderId) && markers.add(message.senderId);
      case READY:
        return clocks.putIfAbsent(message.senderId, message.clock) == null;
      case CONFIRM:
        return message.applied && confirmations.putIfAbsent(message.senderId, message) == null;
      default:
        return false;
    }
  }

  public boolean allMarkers() { 
	  return markers.containsAll(members.keySet()); 
  }
  public boolean allClocks() { 
	  return clocks.keySet().containsAll(participants.keySet()); 
  }

  public long chooseBoundary() {
    if (!allClocks())
      throw new IllegalStateException("waiting for final clocks");
    
    long maximum = 0;
    
    for (long clock : clocks.values())
      maximum = Math.max(maximum, clock);
    return maximum;
  }

  public boolean allInstalled() {
    return applied && matching(confirmations, participants.keySet(), true);
  }

  public void beginRecovery(Map<String, Long> remaining) {
    survivors = new HashMap<>(remaining);
    statuses.clear();
    recovered.clear();
    recovering = true;
    resolved = false;
  }

  private boolean remainingMembersApplied() {
    if (request.operation() != MembershipRequest.Operation.LEAVE)
      return false;
    for (String actor : survivors.keySet()) {
      if (actor.equals(request.actorId()))
        continue;
      MembershipMessage status = statuses.get(actor);
      if (status == null || !status.applied)
        return false;
    }
    return true;
  }

  public boolean everyoneReported() {
    return statuses.keySet().containsAll(survivors.keySet()) || remainingMembersApplied();
  }

  public void resolve() {
    if (!everyoneReported())
      throw new IllegalStateException("waiting for survivor states");
    boundary = -1;
    for (MembershipMessage status : statuses.values()) {
      if (status.applied) {
        if (boundary >= 0 && boundary != status.clock)
          throw new IllegalStateException("membership clocks disagree");
        boundary = status.clock;
      }
    }
    resolved = true;
  }

  public boolean recoveryComplete() {
    Set<String> required = new HashSet<>(survivors.keySet());
    // A leaver may have closed its queues after all remaining members applied the change.
    if (remainingMembersApplied())
      required.remove(request.actorId());
    return resolved && matching(recovered, required, boundary >= 0);
  }

  private boolean matching(Map<String, MembershipMessage> replies, Set<String> required, boolean kept) {
    for (String actor : required) {
      MembershipMessage reply = replies.get(actor);
      if (reply == null || reply.applied != kept || (kept && reply.clock != boundary))
        return false;
    }
    return true;
  }
}
