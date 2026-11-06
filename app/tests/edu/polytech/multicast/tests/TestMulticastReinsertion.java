package edu.polytech.multicast.tests;

import java.util.ArrayList;
import java.util.List;

import edu.polytech.multicast.Actor;
import edu.polytech.multicast.GroupManager;
import edu.polytech.multicast.GroupManager.Started;
import edu.polytech.multicast.Verifier;
import edu.polytech.queues.Task;

import java.util.concurrent.CopyOnWriteArrayList;

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
    // first, while the JVM is cold: this is when the timing problems it looks for are the most likely
    scenario("dense load during crash, resurrection and reinsertion (8 rounds)", () -> {
      for (int round = 0; round < 8; round++)
        denseLoad();
    });
    for (final int period : periods) {
      scenario("suicide, resurrection, reinsertion, period " + period, () -> cycles(false, period, 1));
      scenario("random failure, resurrection, reinsertion, period " + period, () -> cycles(true, period, 1));
      scenario("suicide twice, reinserted twice, period " + period, () -> cycles(false, period, 2));
      scenario("resurrected but not reinserted, period " + period, () -> respawnedOutsideTheView(period));
      scenario("reinsertion while the survivors keep multicasting, period " + period, () -> reinsertionUnderLoad(period));
    }
    scenario("respawn and reinsert before the crash flush has ended (5 rounds)", () -> {
      for (int round = 0; round < 5; round++)
        earlyRespawn();
    });
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
      awaitDelivered(verifier, allIds(started), expected);
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
    awaitDelivered(verifier, survivors, expected);
    if (!verifier.awaitQuiet(quiet(period), 4 * TIMEOUT_MS))
      throw new AssertionError("deliveries never stopped");

    expect(victim.state() == Actor.State.RESPAWNED, "the actor must stay RESPAWNED");
    expect(verifier.deliveredCount(victim.id()) == deliveredBefore, "an actor outside the view delivered something");
    verifier.checkSurvivors(survivors);
    for (String survivor : survivors)
      expectDeliveredOnce(verifier, survivor, expected); // the survivors are not blocked by the absent actor
  }
  
  /**
   * The hard case: the survivors multicast without stopping before, during and after the crash, the
   * resurrection and the reinsertion. Messages are in flight while the view changes.
   */
  private static void reinsertionUnderLoad(int period) throws Exception {
    final Started started = GroupManager.start(ACTORS, period, 200);
    Verifier verifier = started.verifier();
    final Actor victim = started.actors().get(ACTORS - 1);
    final List<String> survivors = survivorIds(started, victim);
    final List<String> flow = new CopyOnWriteArrayList<String>();

    Thread flowThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          for (int i = 0; i < 600; i++) {
            for (Actor actor : started.actors()) {
              if (actor == victim)
                continue;
              String payload = "flow-" + actor.id() + "#" + i;
              flow.add(payload);
              actor.multicast(payload.getBytes());
            }
            Thread.sleep(1);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
    flowThread.start();

    crash(started, victim, false, 0);
    if (!verifier.awaitCrashed(victim.id(), ACTORS - 1, TIMEOUT_MS))
      throw new AssertionError("the crash of " + victim.id() + " was not noticed by every survivor");
    victim.respawn();
    awaitState(victim, Actor.State.RESPAWNED);
    victim.reinsert();
    if (!verifier.awaitReinserted(victim.id(), TIMEOUT_MS))
      throw new AssertionError(victim.id() + " was not reinserted");
    // the actor that came back multicasts at once, while the others may not have installed the view yet
    List<String> immediate = new ArrayList<String>();
    for (int i = 0; i < 20; i++) {
      String payload = "immediate-" + victim.id() + "#" + i;
      immediate.add(payload);
      victim.multicast(payload.getBytes());
    }
    flowThread.join();

    List<String> expected = multicastAfter(started, 1);
    expected.addAll(immediate);
    List<String> everything = new ArrayList<String>(flow);
    everything.addAll(expected);
    awaitDelivered(verifier, survivors, everything);
    awaitDelivered(verifier, allIds(started), expected);
    if (!verifier.awaitQuiet(500, 4 * TIMEOUT_MS))
      throw new AssertionError("deliveries never stopped");

    verifier.checkSurvivors(survivors);
    verifier.checkReinserted(survivors, victim.id());
    for (String survivor : survivors)
      expectDeliveredOnce(verifier, survivor, everything);
    expectDeliveredOnce(verifier, victim.id(), expected);
  }

  /**
   * The survivors multicast as fast as they can, from before the crash until after the reinsertion,
   * and the actor that comes back multicasts at once. There is NO extra wave afterwards: every message
   * must be delivered by itself, with the acknowledgements sent around the view change.
   * (An acknowledgement sent just before a member installed the new view never reached the new
   * member. With more traffic afterwards this stayed hidden, without it the actor delivered nothing.)
   */
  private static void denseLoad() throws Exception {
    final Started started = GroupManager.start(ACTORS, 0, 50);
    Verifier verifier = started.verifier();
    final Actor victim = started.actors().get(ACTORS - 1);
    final List<String> survivors = survivorIds(started, victim);
    final List<String> flow = new CopyOnWriteArrayList<String>();

    Thread flowThread = new Thread(new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < 1500; i++) {
          for (Actor actor : started.actors()) {
            if (actor == victim)
              continue;
            String payload = "dense-" + actor.id() + "#" + i;
            flow.add(payload);
            actor.multicast(payload.getBytes());
          }
        }
      }
    });
    flowThread.start();

    started.actors().get(0).task().post(new Runnable() {
      @Override
      public void run() {
        victim.task().kill();
      }
    }, 5); // early, while the flow is at its peak
    if (!verifier.awaitCrashed(victim.id(), ACTORS - 1, TIMEOUT_MS))
      throw new AssertionError("the crash of " + victim.id() + " was not noticed by every survivor");
    victim.respawn();
    awaitState(victim, Actor.State.RESPAWNED);
    victim.reinsert();
    if (!verifier.awaitReinserted(victim.id(), TIMEOUT_MS))
      throw new AssertionError(victim.id() + " was not reinserted");
    List<String> own = new ArrayList<String>();
    for (int i = 0; i < 30; i++) {
      String payload = "own-" + victim.id() + "#" + i;
      own.add(payload);
      victim.multicast(payload.getBytes());
    }
    flowThread.join();

    List<String> everything = new ArrayList<String>(flow);
    everything.addAll(own);
    awaitDelivered(verifier, survivors, everything);
    awaitDelivered(verifier, java.util.Collections.singletonList(victim.id()), own);
    if (!verifier.awaitQuiet(500, 4 * TIMEOUT_MS))
      throw new AssertionError("deliveries never stopped");
    verifier.checkSurvivors(survivors);
    verifier.checkReinserted(survivors, victim.id());
    expectDeliveredOnce(verifier, victim.id(), own);
  }

  /**
   * The actor comes back right after its death, while some survivors may not have finished the crash
   * flush yet. A survivor that still has it in its view must wait for the end of that flush before it
   * accepts the reinsertion.
   */
  private static void earlyRespawn() throws Exception {
    Started started = GroupManager.start(ACTORS, 0, 100);
    Verifier verifier = started.verifier();
    Actor victim = started.actors().get(ACTORS - 1);
    List<String> survivors = survivorIds(started, victim);

    crash(started, victim, false, 0);
    awaitState(victim, Actor.State.DEAD); // not awaitCrashed: the survivors may still be flushing
    victim.respawn();
    awaitState(victim, Actor.State.RESPAWNED);
    victim.reinsert();
    if (!verifier.awaitReinserted(victim.id(), TIMEOUT_MS))
      throw new AssertionError(victim.id() + " was not reinserted");

    List<String> expected = multicastAfter(started, 1);
    awaitDelivered(verifier, allIds(started), expected);
    if (!verifier.awaitQuiet(500, 4 * TIMEOUT_MS))
      throw new AssertionError("deliveries never stopped");
    verifier.checkSurvivors(survivors);
    verifier.checkReinserted(survivors, victim.id());
    for (Actor actor : started.actors())
      expectDeliveredOnce(verifier, actor.id(), expected);
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
  
  private static List<String> allIds(Started started) {
	    List<String> ids = new ArrayList<String>();
	    for (Actor actor : started.actors())
	      ids.add(actor.id());
	    return ids;
	  }

	  /**
	   * Waits until every listed actor has delivered every expected payload.
	   * (awaitQuiet alone is not enough: it returns at once when nothing was delivered for a while,
	   * before the messages we just asked for are even sent.)
	   */
	  private static void awaitDelivered(Verifier verifier, List<String> actorIds, List<String> expected)
	      throws InterruptedException {
	    long deadline = System.currentTimeMillis() + 4 * TIMEOUT_MS;
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
	        throw new AssertionError(missing + " did not deliver every message multicast after the reinsertion");
	      Thread.sleep(20);
	    }
	  }
}
