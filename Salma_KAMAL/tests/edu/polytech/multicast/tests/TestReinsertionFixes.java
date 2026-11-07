package edu.polytech.multicast.tests;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.polytech.multicast.Actor;
import edu.polytech.multicast.GroupManager;
import edu.polytech.multicast.GroupManager.Started;
import edu.polytech.multicast.Verifier;

/**
 * Two actors come back and must agree on the same view, a reinsertion that dies
 * must not freeze the survivors, and a message delivered during the absence is missed forever.
 */
public class TestReinsertionFixes {

  public static void main(String[] args) throws Exception {
    try {
      gapDuringAbsenceIsRejected();
      twoReturneesAgree();
      deathDuringReinsertionDoesNotFreeze();
    } catch (Throwable failure) {
      failure.printStackTrace(System.err);
      System.exit(1);
    }
    System.out.println("reinsertion fix tests: PASS");
    System.exit(0);
  }

  /** A gap delivered by the others after resurrection and before reinsertion must be rejected. */
  private static void gapDuringAbsenceIsRejected() {
    Verifier verifier = new Verifier();
    verifier.delivered("alice", 1L, "before".getBytes());
    verifier.delivered("bob", 1L, "before".getBytes());
    verifier.resurrected("bob");
    verifier.delivered("alice", 2L, "gap".getBytes());
    verifier.reinserted("bob");
    verifier.delivered("alice", 3L, "after".getBytes());
    verifier.delivered("bob", 2L, "gap".getBytes());
    verifier.delivered("bob", 3L, "after".getBytes());
    List<String> survivors = new ArrayList<String>();
    survivors.add("alice");
    boolean rejected = false;
    try {
      verifier.checkReinserted(survivors, "bob");
    } catch (AssertionError error) {
      rejected = error.getMessage() != null && error.getMessage().contains("missed");
    }
    if (!rejected)
      throw new AssertionError("a message delivered during the absence was accepted");
  }

  private static void twoReturneesAgree() throws Exception {
    Started started = GroupManager.start(3, 0, 1);
    final List<Actor> actors = started.actors();
    final Actor alice = actors.get(0);
    final Actor bob = actors.get(1);
    final Actor carol = actors.get(2);

    alice.task().post(new Runnable() {
      @Override
      public void run() {
        bob.task().kill();
      }
    });
    awaitView(alice, ids(alice, carol));
    alice.task().post(new Runnable() {
      @Override
      public void run() {
        carol.task().kill();
      }
    });
    awaitView(alice, ids(alice));

    bob.respawn();
    carol.respawn();
    awaitState(bob, Actor.State.RESPAWNED);
    awaitState(carol, Actor.State.RESPAWNED);
    carol.reinsert();
    awaitState(carol, Actor.State.IN_VIEW);
    bob.reinsert();
    awaitState(bob, Actor.State.IN_VIEW);

    List<String> everyone = ids(alice, bob, carol);
    awaitView(alice, everyone);
    awaitView(bob, everyone);
    awaitView(carol, everyone);

    alice.multicast("after-both".getBytes());
    List<String> expected = new ArrayList<String>();
    expected.add("after-both");
    awaitDelivered(started.verifier(), everyone, expected);
  }

  private static void deathDuringReinsertionDoesNotFreeze() throws Exception {
    Started started = GroupManager.start(3, 0, 1);
    final List<Actor> actors = started.actors();
    final Actor alice = actors.get(0);
    final Actor bob = actors.get(1);
    final Actor victim = actors.get(2);

    alice.task().post(new Runnable() {
      @Override
      public void run() {
        victim.task().kill();
      }
    });
    awaitView(alice, ids(alice, bob));
    victim.respawn();
    awaitState(victim, Actor.State.RESPAWNED);
    victim.reinsert();
    victim.task().post(new Runnable() {
      @Override
      public void run() {
        victim.task().kill();
      }
    });
    awaitState(victim, Actor.State.DEAD);
    Thread.sleep(400);

    alice.multicast("after-death".getBytes());
    bob.multicast("after-death-2".getBytes());
    List<String> expected = new ArrayList<String>();
    expected.add("after-death");
    expected.add("after-death-2");
    awaitDelivered(started.verifier(), ids(alice, bob), expected);
  }

  private static List<String> ids(Actor... actors) {
    List<String> ids = new ArrayList<String>();
    for (Actor actor : actors)
      ids.add(actor.id());
    Collections.sort(ids);
    return ids;
  }

  private static void awaitView(Actor actor, List<String> expected) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 8000L;
    while (true) {
      List<String> view = new ArrayList<String>(actor.view());
      Collections.sort(view);
      if (view.equals(expected))
        return;
      if (System.currentTimeMillis() > deadline)
        throw new AssertionError(actor.id() + " view " + view + " expected " + expected);
      Thread.sleep(20);
    }
  }

  private static void awaitState(Actor actor, Actor.State state) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 8000L;
    while (actor.state() != state) {
      if (System.currentTimeMillis() > deadline)
        throw new AssertionError(actor.id() + " did not reach " + state + " (state is " + actor.state() + ")");
      Thread.sleep(10);
    }
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
        throw new AssertionError(missing + " did not deliver " + expected
            + " (has " + verifier.deliveredPayloads(missing) + ")");
      Thread.sleep(20);
    }
  }
}