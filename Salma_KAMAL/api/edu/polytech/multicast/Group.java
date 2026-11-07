package edu.polytech.multicast;

import java.util.ArrayList;
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
}