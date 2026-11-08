package edu.polytech.multicast.tests;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.polytech.multicast.Actor;
import edu.polytech.multicast.GroupListener;
import edu.polytech.multicast.GroupManager;
import edu.polytech.multicast.GroupManager.Started;
import edu.polytech.queues.Task;

/** One membership request at a time, with configurable load and failure mode. */
public class TestMulticastMembership {
  private static final long TIMEOUT_MS = 10000;

  public static void main(String[] args) throws Exception {
    try {
      if (args.length > 0) {
        runDynamic(Integer.parseInt(args[0]), args.length > 1 && args[1].equals("random"));
      } else {
        runDynamic(100, false);
        runDynamic(0, false);
        runDynamic(100, true);
        runDynamic(0, true);
        runPendingLeave();
        runInterruptedLeave();
        runLastMemberLeaves();
      }
    } catch (Throwable failure) {
      failure.printStackTrace(System.err);
      System.exit(1);
    } finally {
      Task.setFailureProbability(0);
    }
    System.out.println("multicast membership tests: PASS");
    System.exit(0);
  }

  private static void runDynamic(int period, boolean random) throws Exception {
    int messagesPerActor = period == 0 ? 200 : GroupManager.MESSAGES_PER_ACTOR;
    Started started = GroupManager.start(3, period, messagesPerActor);
    List<String> beforeJoin = started.verifier().deliveredPayloads(started.actors().get(0).id());
    List<Actor> actors = new ArrayList<>(started.actors());
    Actor newcomer = actors.get(0).newActor("new-" + actors.get(0).id());
    checkStateDuringNotifications(newcomer);
    actors.add(newcomer);
    newcomer.join(actors.get(0).id());
    awaitChange(actors, newcomer, Actor.State.IN_VIEW);
    check(Collections.disjoint(beforeJoin, started.verifier().deliveredPayloads(newcomer.id())),
        "new member received old messages");
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    for (Actor actor : started.actors()) {
      String lastMessage = actor.id() + "#" + (messagesPerActor - 1);
      while (!started.verifier().deliveredPayloads(actor.id()).contains(lastMessage)) {
        check(System.currentTimeMillis() < deadline, "initial load did not finish");
        Thread.sleep(10);
      }
    }
    sendRound(started, actors, "joined");

    Actor leaver = actors.get(1);
    checkStateDuringNotifications(leaver);
    leaver.leave();
    awaitChange(actors, leaver, Actor.State.LEFT);
    check(!leaver.task().dead() && started.verifier().hasLeft(leaver.id()), "leave was treated as a crash");
    sendRound(started, actors, "left");

    final Actor victim = actors.get(2);
    if (random) {
      Task.setFailureProbability(0.4);
      victim.task().setMayFail(true);
      victim.task().post(new Runnable() {
        public void run() { victim.task().post(this, 5); }
      });
    } else {
      actors.get(0).task().post(new Runnable() {
        public void run() { victim.task().kill(); }
      });
    }
    try {
      check(started.verifier().awaitCrashed(victim.id(), 2, TIMEOUT_MS), "crash was not handled");
    } finally {
      Task.setFailureProbability(0);
    }
    sendRound(started, actors, "absent");
    victim.respawn();
    awaitState(victim, Actor.State.RESPAWNED);
    victim.reinsert();
    check(started.verifier().awaitReinserted(victim.id(), TIMEOUT_MS), "reinsertion did not finish");
    awaitChange(actors, victim, Actor.State.IN_VIEW);
    sendRound(started, actors, "reinserted");
    List<String> ids = new ArrayList<>();
    for (Actor actor : actors) ids.add(actor.id());
    started.verifier().checkMembership(ids);
    newcomer.leave();
    awaitChange(actors, newcomer, Actor.State.LEFT);
    sendRound(started, actors, "finished");
  }

  private static void runPendingLeave() throws Exception {
    final Started started = GroupManager.start(3, 0);
    final Actor leaver = started.actors().get(2);
    leaver.task().post(new Runnable() {
      public void run() {
        for (int i = 0; i < 10; i++) leaver.multicast(("pending " + i).getBytes());
        leaver.leave();
      }
    });
    awaitChange(started.actors(), leaver, Actor.State.LEFT);
    for (Actor actor : started.actors())
      for (int i = 0; i < 10; i++)
        check(Collections.frequency(started.verifier().deliveredPayloads(actor.id()), "pending " + i) == 1,
            "pending message lost or repeated");
    sendRound(started, started.actors(), "after pending leave");
  }

  private static void runInterruptedLeave() throws Exception {
    final Started started = GroupManager.start(3, 0);
    final Actor leaver = started.actors().get(2);
    leaver.task().post(new Runnable() {
      public void run() {
        leaver.leave();
        leaver.task().post(new Runnable() {
          public void run() { leaver.task().kill(); }
        });
      }
    });
    check(started.verifier().awaitCrashed(leaver.id(), 2, TIMEOUT_MS), "interrupted leave did not handle the crash");
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    for (Actor actor : started.actors()) {
      if (actor == leaver) continue;
      while (!actor.membershipComplete()) {
        check(System.currentTimeMillis() < deadline, "interrupted leave did not finish");
        Thread.sleep(10);
      }
    }
    sendRound(started, started.actors(), "after interrupted leave");
  }

  private static void runLastMemberLeaves() throws Exception {
    Started started = GroupManager.start(1, 0);
    Actor leaver = started.actors().get(0);
    checkStateDuringNotifications(leaver);
    leaver.leave();
    awaitChange(started.actors(), leaver, Actor.State.LEFT);
    check(leaver.view().isEmpty() && !leaver.task().dead(), "last member did not leave gracefully");
  }

  private static void awaitState(Actor actor, Actor.State state) throws Exception {
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    while (actor.state() != state) {
      check(System.currentTimeMillis() < deadline, actor.id() + " did not become " + state);
      Thread.sleep(10);
    }
  }

  private static void checkStateDuringNotifications(final Actor actor) {
    actor.addListener(new GroupListener() {
      public void joined(String actorId) {
        if (actor.id().equals(actorId))
          check(actor.state() == Actor.State.IN_VIEW, "join listener saw an unfinished state");
      }
      public void left(String actorId) {
        if (actor.id().equals(actorId))
          check(actor.state() == Actor.State.LEFT, "leave listener saw an unfinished state");
      }
      public void crashed(String actorId) { }
      public void resurrected(String actorId) { }
      public void reinserted(String actorId) { }
    });
  }

  private static void awaitChange(List<Actor> actors, Actor subject, Actor.State state) throws Exception {
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    while (true) {
      boolean finished = subject.state() == state && subject.membershipComplete();
      for (Actor actor : actors) {
        if (actor.task().dead() || actor.state() == Actor.State.LEFT)
          continue;
        finished &= actor.membershipComplete();
        finished &= actor.view().contains(subject.id()) == (state == Actor.State.IN_VIEW);
      }
      if (finished) return;
      check(System.currentTimeMillis() < deadline, "membership change did not finish for " + subject.id());
      Thread.sleep(10);
    }
  }

  private static void sendRound(Started started, List<Actor> actors, String round) throws Exception {
    List<String> payloads = new ArrayList<>();
    List<String> ids = new ArrayList<>();
    for (Actor actor : actors) {
      if (actor.task().dead() || actor.state() != Actor.State.IN_VIEW) continue;
      ids.add(actor.id());
      String payload = round + " " + actor.id();
      payloads.add(payload);
      actor.multicast(payload.getBytes());
    }
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    for (String id : ids) {
      while (!started.verifier().deliveredPayloads(id).containsAll(payloads)) {
        check(System.currentTimeMillis() < deadline, "messages did not reach " + id);
        Thread.sleep(10);
      }
      for (String payload : payloads)
        check(Collections.frequency(started.verifier().deliveredPayloads(id), payload) == 1, "duplicate delivery");
    }
    started.verifier().checkMembership(ids);
  }

  private static void check(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
  }
}
