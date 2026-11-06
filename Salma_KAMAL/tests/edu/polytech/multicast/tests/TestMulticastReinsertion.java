package edu.polytech.multicast.tests;

import java.util.ArrayList;
import java.util.List;

import edu.polytech.multicast.Actor;
import edu.polytech.multicast.GroupManager;
import edu.polytech.multicast.GroupManager.Started;
import edu.polytech.multicast.Verifier;
import edu.polytech.queues.Task;

/**
 * Part 3: resurrection and reinsertion.
 * Run from the Eclipse project directory:
 *   javac -d bin $(find api local utils.queues tests -name '*.java')
 *   java -cp bin edu.polytech.multicast.tests.TestMulticastReinsertion          (periods 200 then 0)
 *   java -cp bin edu.polytech.multicast.tests.TestMulticastReinsertion 50       (only period 50)
 * Every scenario is run and reported. The exit code is 1 if one of them fails.
 */
public class TestMulticastReinsertion {

  private static final int ACTORS = 3;
  private static final int AFTER = 5; // messages multicast by each actor once the victim is back
  private static final long TIMEOUT_MS = 10000L;

  private interface Action {
    void run() throws Exception;
  }

  private static int failures;

  public static void main(String[] args) throws Exception {
    int[] periods = args.length > 0 ? new int[] { Integer.parseInt(args[0]) } : new int[] { 200, 0 };
    for (final int period : periods) {
      scenario("suicide, resurrection, reinsertion, period " + period, () -> cycles(false, period, 1));
      scenario("random failure, resurrection, reinsertion, period " + period, () -> cycles(true, period, 1));
      scenario("suicide twice, reinserted twice, period " + period, () -> cycles(false, period, 2));
      scenario("resurrected but not reinserted, period " + period, () -> respawnedOutsideTheView(period));
    }
    scenario("calls in the wrong state", () -> wrongStates());
    if (failures > 0) {
      System.out.println("multicast reinsertion tests: FAIL (" + failures + " scenario(s))");
      System.exit(1);
    }
    System.out.println("multicast reinsertion tests: PASS");
    System.exit(0);
  }

  private static void scenario(String name, Action action) {
    try {
      action.run();
      System.out.println("[PASS] " + name);
    } catch (Throwable t) {
      failures++;
      System.out.println("[FAIL] " + name + " -> " + t);
    } finally {
      Task.setFailureProbability(0);
    }
  }

  /**
   * One actor (the last one) dies, comes back empty, is reinserted, and then everybody multicasts again.
   * Repeated 'cycles' times.
   */
  private static void cycles(boolean random, int period, int cycles) throws Exception {
    int load = period == 0 ? 300 : 12;
    Started started = GroupManager.start(ACTORS, period, load);
    Verifier verifier = started.verifier();
    Actor victim = started.actors().get(ACTORS - 1);
    List<String> survivors = survivorIds(started, victim);

    for (int life = 1; life <= cycles; life++) {
      crash(started, victim, random && life == 1, period);
      if (!verifier.awaitCrashed(victim.id(), life * (ACTORS - 1), TIMEOUT_MS))
        throw new AssertionError("the crash of " + victim.id() + " was not noticed by every survivor");
      Task.setFailureProbability(0);

      victim.respawn();
      awaitState(victim, Actor.State.RESPAWNED);
      expect(victim.generationId() == life + 1, "generationId must be " + (life + 1));

      victim.reinsert();
      if (!verifier.awaitReinserted(victim.id(), TIMEOUT_MS))
        throw new AssertionError(victim.id() + " was not reinserted");
      expect(victim.state() == Actor.State.IN_VIEW, "state must be IN_VIEW after reinsertion");

      List<String> expected = multicastAfter(started, life);
      if (!verifier.awaitQuiet(quiet(period), 4 * TIMEOUT_MS))
        throw new AssertionError("deliveries never stopped");

      verifier.checkSurvivors(survivors);
      verifier.checkReinserted(survivors, victim.id());
      for (Actor actor : started.actors())
        expectDeliveredOnce(verifier, actor.id(), expected);
    }
  }

  /**
   * Resurrected is not reinserted: the actor is alive but outside the view.
   * Nobody waits for it, nobody sends it anything, and it delivers nothing.
   */
  private static void respawnedOutsideTheView(int period) throws Exception {
    int load = period == 0 ? 300 : 12;
    Started started = GroupManager.start(ACTORS, period, load);
    Verifier verifier = started.verifier();
    Actor victim = started.actors().get(ACTORS - 1);
    List<String> survivors = survivorIds(started, victim);

    crash(started, victim, false, period);
    if (!verifier.awaitCrashed(victim.id(), ACTORS - 1, TIMEOUT_MS))
      throw new AssertionError("the crash of " + victim.id() + " was not noticed by every survivor");
    victim.respawn();
    awaitState(victim, Actor.State.RESPAWNED);
    expect(!victim.task().dead(), "the new task must be alive");
    expect(victim.generationId() == 2, "generationId must be 2");
    int deliveredBefore = verifier.deliveredCount(victim.id());

    List<String> expected = new ArrayList<String>();
    for (Actor actor : started.actors()) {
      if (actor == victim)
        continue;
      for (int i = 0; i < AFTER; i++) {
        String payload = "outside-" + actor.id() + "#" + i;
        expected.add(payload);
        actor.multicast(payload.getBytes());
      }
    }
    if (!verifier.awaitQuiet(quiet(period), 4 * TIMEOUT_MS))
      throw new AssertionError("deliveries never stopped");

    expect(victim.state() == Actor.State.RESPAWNED, "the actor must stay RESPAWNED");
    expect(verifier.deliveredCount(victim.id()) == deliveredBefore, "an actor outside the view delivered something");
    verifier.checkSurvivors(survivors);
    for (String survivor : survivors)
      expectDeliveredOnce(verifier, survivor, expected); // the survivors are not blocked by the absent actor
  }

  private static void wrongStates() throws Exception {
    Started started = GroupManager.start(ACTORS, 100, 20);
    Verifier verifier = started.verifier();
    final Actor alive = started.actors().get(0);
    final Actor victim = started.actors().get(ACTORS - 1);

    expectIllegalState("respawn() on a live actor", () -> alive.respawn());
    expectIllegalState("reinsert() on a live actor", () -> alive.reinsert());

    crash(started, victim, false, 100);
    if (!verifier.awaitCrashed(victim.id(), ACTORS - 1, TIMEOUT_MS))
      throw new AssertionError("the crash was not noticed");
    expectIllegalState("reinsert() on a dead actor", () -> victim.reinsert());

    victim.respawn();
    awaitState(victim, Actor.State.RESPAWNED);
    expectIllegalState("respawn() twice", () -> victim.respawn());
  }

  // ---------------------------------------------------------------- helpers

  private static List<String> survivorIds(Started started, Actor victim) {
    List<String> ids = new ArrayList<String>();
    for (Actor actor : started.actors())
      if (actor != victim)
        ids.add(actor.id());
    return ids;
  }

  /** Suicide: another actor kills the victim. Random: the victim becomes failable. */
  private static void crash(Started started, final Actor victim, boolean random, int period) {
    if (random) {
      Task.setFailureProbability(0.4);
      victim.task().setMayFail(true);
      return;
    }
    started.actors().get(0).task().post(new Runnable() {
      @Override
      public void run() {
        victim.task().kill();
      }
    }, period == 0 ? 20 : 300);
  }

  /** Every actor, the victim included, multicasts AFTER messages. Returns their payloads. */
  private static List<String> multicastAfter(Started started, int life) {
    List<String> payloads = new ArrayList<String>();
    for (Actor actor : started.actors()) {
      for (int i = 0; i < AFTER; i++) {
        String payload = "after" + life + "-" + actor.id() + "#" + i;
        payloads.add(payload);
        actor.multicast(payload.getBytes());
      }
    }
    return payloads;
  }

  private static void expectDeliveredOnce(Verifier verifier, String actorId, List<String> expected) {
    List<String> delivered = verifier.deliveredPayloads(actorId);
    for (String payload : expected) {
      int count = 0;
      for (String d : delivered)
        if (d.equals(payload))
          count++;
      if (count != 1)
        throw new AssertionError(actorId + " delivered \"" + payload + "\" " + count + " time(s), expected once");
    }
  }

  private static long quiet(int period) {
    return period == 0 ? 500 : 800;
  }

  private static void awaitState(Actor actor, Actor.State state) throws InterruptedException {
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    while (actor.state() != state) {
      if (System.currentTimeMillis() > deadline)
        throw new AssertionError(actor.id() + " did not reach " + state + " (state is " + actor.state() + ")");
      Thread.sleep(10);
    }
  }

  private static void expect(boolean condition, String message) {
    if (!condition)
      throw new AssertionError(message);
  }

  private static void expectIllegalState(String what, Action action) throws Exception {
    try {
      action.run();
    } catch (IllegalStateException expected) {
      return;
    } catch (Throwable t) {
      throw new AssertionError(what + ": expected IllegalStateException but got " + t);
    }
    throw new AssertionError(what + ": expected IllegalStateException but nothing was thrown");
  }
}
