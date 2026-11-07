package edu.polytech.multicast;

/**
 * Membership events. This part does not emit any of them.
 * Later parts report crashes, resurrection, reinsertion, join, and leave here.
 */
public interface GroupListener {

  void joined(String actorId);

  void left(String actorId);

  void crashed(String actorId);

  void resurrected(String actorId);

  void reinserted(String actorId);
}
