package edu.polytech.multicast.tests;

import java.util.HashMap;
import java.util.Map;

import edu.polytech.multicast.MembershipChange;
import edu.polytech.multicast.MembershipMessage;
import edu.polytech.multicast.MembershipMessage.Type;
import edu.polytech.multicast.MembershipRequest;
import edu.polytech.multicast.MembershipRequest.Operation;

/** Main rules of the single membership-change tracker. */
public class TestMembership {
  public static void main(String[] args) {
    try {
      runFlush();
      runRecovery(false);
      runRecovery(true);
    } catch (Throwable failure) {
      failure.printStackTrace(System.err);
      System.exit(1);
    }
    System.out.println("membership tests: PASS");
    System.exit(0);
  }

  private static Map<String, Long> members() {
    Map<String, Long> members = new HashMap<>();
    members.put("Alice", 1L);
    members.put("Bob", 1L);
    return members;
  }

  private static MembershipChange change() {
    return new MembershipChange(new MembershipRequest("Bob:leave", Operation.LEAVE, "Bob", 1, 0), members());
  }

  private static MembershipMessage reply(MembershipChange change, Type type, String sender, long clock,
      Map<String, Long> survivors, boolean applied) {
    MembershipMessage message = new MembershipMessage(type, change.request, sender, 1,
        members(), survivors, clock, applied);
    return MembershipMessage.decode(message.encode());
  }

  private static void runFlush() {
    MembershipChange change = change();
    MembershipMessage alice = reply(change, Type.FLUSH, "Alice", 10, new HashMap<>(), false);
    check(change.record(alice) && !change.record(alice), "marker must count once");
    check(!change.allMarkers(), "did not wait for Bob's marker");
    change.record(reply(change, Type.FLUSH, "Bob", 20, new HashMap<>(), false));
    check(change.allMarkers(), "markers were lost");
    change.record(reply(change, Type.READY, "Alice", 30, new HashMap<>(), false));
    check(!change.allClocks(), "did not wait for Bob's final clock");
    change.record(reply(change, Type.READY, "Bob", 40, new HashMap<>(), false));
    change.boundary = change.chooseBoundary();
    check(change.boundary == 40, "wrong maximum clock");
    change.applied = true;
    change.record(reply(change, Type.CONFIRM, "Alice", 40, new HashMap<>(), true));
    check(!change.allInstalled(), "finished before every member confirmed");
    change.record(reply(change, Type.CONFIRM, "Bob", 40, new HashMap<>(), true));
    check(change.allInstalled() && !change.nextMembers.containsKey("Bob"), "leave did not complete");
  }

  private static void runRecovery(boolean alreadyApplied) {
    MembershipChange change = change();
    change.applied = alreadyApplied;
    Map<String, Long> survivors = members();
    survivors.remove("Bob");
    change.beginRecovery(survivors);
    change.record(reply(change, Type.STATUS, "Alice", alreadyApplied ? 40 : -1, survivors, alreadyApplied));
    check(change.everyoneReported(), "kept waiting for failed Bob");
    change.resolve();
    check((change.boundary == 40) == alreadyApplied, "recovery chose the wrong outcome");
    MembershipMessage late = reply(change, Type.CONFIRM, "Alice", 40, members(), true);
    check(!change.record(late), "reply for another survivor set was accepted");
    change.record(reply(change, Type.CONFIRM, "Alice", change.boundary, survivors, alreadyApplied));
    check(change.recoveryComplete(), "survivor confirmation did not complete recovery");
  }

  private static void check(boolean condition, String message) {
    if (!condition)
      throw new AssertionError(message);
  }
}
