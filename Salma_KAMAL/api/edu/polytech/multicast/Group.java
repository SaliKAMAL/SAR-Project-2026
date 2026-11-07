package edu.polytech.multicast;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * The set of member ids, sorted so every actor sees the same order.
 * In Part 2, this group shrinks when a member is confirmed dead after a flush.
 */
public final class Group {
  private final List<String> members;

  Group(List<String> memberIds) {
    List<String> sorted = new ArrayList<String>(memberIds);
    Collections.sort(sorted);
    // CRUCIAL : La liste doit être mutable pour la Partie 2 !
    this.members = sorted;
  }

  public List<String> members() {
    return members;
  }

  /**
   * Removes a crashed actor from the current view.
   */
  public void remove(String actorId) {
    members.remove(actorId);
  }
  
  /** Part 3: adds a reinserted actor to the view. The list stays sorted. */
  public void add(String actorId) {
    if (!members.contains(actorId)) {
      members.add(actorId);
      Collections.sort(members);
    }
  }

  /** Part 3: the view of a reinserted actor is the view agreed by the flush. */
  void reset(Collection<String> memberIds) {
    members.clear();
    members.addAll(memberIds);
    Collections.sort(members);
  }
}