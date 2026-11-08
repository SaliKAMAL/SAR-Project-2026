package edu.polytech.multicast.tests;

import java.util.ArrayList;
import java.util.List;

import edu.polytech.multicast.Actor;
import edu.polytech.multicast.GroupManager;
import edu.polytech.multicast.GroupManager.Started;
import edu.polytech.multicast.Verifier;

/**
 * Two actors die before either crash flush has finished. The survivors must still
 * shrink the view and deliver a later multicast. A listed survivor with no delivery
 * record must fail the check.
 */
public class TestSimultaneousCrashes {

  public static void main(String[] args) throws Exception {
    try {
      silentSurvivorIsRejected();
      twoCrashesStillDeliver();
    } catch (Throwable failure) {
      failure.printStackTrace(System.err);
      System.exit(1);
    }
    System.out.println("simultaneous crash tests: PASS");
    System.exit(0);
  }

  private static void silentSurvivorIsRejected() {
    Verifier verifier = new Verifier();
    verifier.delivered("a", 1L, "x".getBytes());
    boolean rejected = false;
    try {
      List<String> claimed = new ArrayList<String>();
      claimed.add("a");
      claimed.add("b");
      verifier.checkSurvivors(claimed);
    } catch (AssertionError error) {
      rejected = true;
    }
    if (!rejected)
      throw new AssertionError("a survivor with no delivery record was accepted");
  }

  private static void twoCrashesStillDeliver() throws Exception {
    Started started = GroupManager.start(4, 0, 2);
    final List<Actor> actors = started.actors();
    final Actor first = actors.get(2);
    final Actor second = actors.get(3);
    actors.get(0).task().post(new Runnable() {
      @Override
      public void run() {
        first.task().kill();
        second.task().kill();
      }
    });

    if (!started.verifier().awaitCrashed(first.id(), 2, 8000L))
      throw new AssertionError(first.id() + " was not removed after the double crash");
    if (!started.verifier().awaitCrashed(second.id(), 2, 8000L))
      throw new AssertionError(second.id() + " was not removed after the double crash");

    List<String> survivors = new ArrayList<String>();
    survivors.add(actors.get(0).id());
    survivors.add(actors.get(1).id());
    List<String> expected = new ArrayList<String>();
    for (int i = 0; i < survivors.size(); i++) {
      String payload = "after-" + survivors.get(i);
      expected.add(payload);
      actors.get(i).multicast(payload.getBytes());
    }
    awaitDelivered(started.verifier(), survivors, expected);
    started.verifier().checkSurvivors(survivors);
  }

  private static void awaitDelivered(Verifier verifier, List<String> actorIds, List<String> expected)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + 8000L;
    while (true) {
      String missing = null;
      for (String actorId : actorIds) {
        if (!verifier.deliveredPayloads(actorId).containsAll(expected)) {
          missing = actorId;
          break;
        }
      }
      if (missing == null)
        return;
      if (System.currentTimeMillis() > deadline)
        throw new AssertionError(missing + " did not deliver the messages sent after the double crash");
      Thread.sleep(20);
    }
  }
}
