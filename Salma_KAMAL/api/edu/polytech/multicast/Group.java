package edu.polytech.multicast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The fixed set of member ids, sorted so every actor sees the same order.
 * This part never removes a member.
 */
public final class Group {

  private final List<String> members;

  Group(List<String> memberIds) {
    List<String> sorted = new ArrayList<String>(memberIds);
    Collections.sort(sorted);
    this.members = Collections.unmodifiableList(sorted);
  }

  public List<String> members() {
    return members;
  }
}
