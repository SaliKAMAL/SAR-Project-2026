package edu.polytech.multicast.tests;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import edu.polytech.multicast.Actor;
import edu.polytech.multicast.GroupManager;
import edu.polytech.multicast.GroupManager.Started;
import edu.polytech.multicast.Verifier;
import edu.polytech.queues.Task;

/**
 * Part 3, hard cases of the reinsertion.
 * Run from the Eclipse project directory:
 *   javac -d bin $(find api local utils.queues tests -name '*.java')
 *   java -cp bin edu.polytech.multicast.tests.TestReinsertionHardCases        (every scenario once)
 *   java -cp bin edu.polytech.multicast.tests.TestReinsertionHardCases 5      (every scenario 5 times)
 *
 * 1. the actor that comes back dies while it is being reinserted: the survivors must not be blocked;
 * 2. two actors come back at the same time: every member must end with the same view;
 * 3. a member dies while another actor is being reinserted, or one of two returning actors dies;
 * 4. the membership changed between the resurrection and the reinsertion: the returning actor must
 *    use the current members, not the ones it knew when it came back.
 */
public class TestReinsertionHardCases {

  private static final int AFTER = 5;
  private static final long TIMEOUT_MS = 10000L;

  private interface Action {
    void run() throws Exception;
  }

  private static int failures;
  private static String only;

  public static void main(String[] args) throws Exception {
    int rounds = args.length > 0 ? Integer.parseInt(args[0]) : 1;
    only = args.length > 1 ? args[1] : null; // run only the scenarios whose name contains this text
    for (int round = 0; round < rounds; round++) {
      for (final int delay : new int[] { 0, 1, 2, 3, 5, 8, 13, 21 })
        scenario("returning actor dies " + delay + " ms after reinsert()", () -> dyingDuringReinsertion(delay));
      for (final int delay : new int[] { 0, 1, 3, 8 })
        scenario("a member dies " + delay + " ms after reinsert() of another actor", () -> memberDiesDuringReinsertion(delay));
      for (final int delay : new int[] { 0, 1, 2, 4, 8 })
        scenario("two actors come back, one dies " + delay + " ms after reinsert()", () -> twoReturningOneDies(delay));
      scenario("two actors come back at the same time (4 actors)", () -> twoReturning(4));
      scenario("two actors come back at the same time (3 actors)", () -> twoReturning(3));
      scenario("a member died after the resurrection", () -> staleConnectionMemberDied());
      scenario("a member died and came back after the resurrection", () -> staleConnectionMemberCameBack());
    }
    if (failures > 0) {
      System.out.println("reinsertion hard cases: FAIL (" + failures + " scenario(s))");
      System.exit(1);
    }
    System.out.println("reinsertion hard cases: PASS");
    System.exit(0);
  }

  private static void scenario(String name, Action action) {
    if (only != null && !name.contains(only))
      return;
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

  // ------------------------------------------------------------------ scenarios

  /** The victim dies, comes back, and dies again while it is being reinserted. Then it comes back for good. */
  private static void dyingDuringReinsertion(int delayMs) throws Exception {
    Started started = GroupManager.start(3, 0, 150);
    Verifier verifier = started.verifier();
    Actor victim = started.actors().get(2);
    List<String> survivors = ids(started.actors().get(0), started.actors().get(1));

    kill(started, victim, 20);
    awaitCrashed(verifier, victim, 2);
    comeBack(victim);
    victim.reinsert();
    die(victim, delayMs);
    awaitState(victim, Actor.State.DEAD);

    // the survivors are not blocked: what they multicast now is delivered, in the same order
    List<String> expected = multicastFrom(survivorActors(started, victim), "dying");
    awaitDelivered(verifier, survivors, expected);
    quiet(verifier);
    verifier.checkSurvivors(survivors);

    // and the actor can come back again
    comeBack(victim);
    victim.reinsert();
    awaitReinserted(verifier, victim);
    List<String> again = multicastFrom(started.actors(), "again");
    awaitDelivered(verifier, ids(started.actors()), again);
    quiet(verifier);
    verifier.checkSurvivors(survivors);
    verifier.checkReinserted(survivors, victim.id());
    for (Actor actor : started.actors())
      expectDeliveredOnce(verifier, actor.id(), again);
  }

  /** The victim is reinserted. A member of the view dies at the same moment. */
  private static void memberDiesDuringReinsertion(int delayMs) throws Exception {
    Started started = GroupManager.start(4, 0, 150);
    Verifier verifier = started.verifier();
    Actor victim = started.actors().get(3);
    Actor member = started.actors().get(1);
    List<String> survivors = ids(started.actors().get(0), started.actors().get(2));

    kill(started, victim, 20);
    awaitCrashed(verifier, victim, 3);
    comeBack(victim);
    victim.reinsert();
    die(member, delayMs);
    awaitReinserted(verifier, victim);
    if (!verifier.awaitCrashed(member.id(), 2, TIMEOUT_MS))
      throw new AssertionError("the crash of " + member.id() + " was not noticed by the survivors");

    List<Actor> alive = new ArrayList<Actor>();
    alive.add(started.actors().get(0));
    alive.add(started.actors().get(2));
    alive.add(victim);
    List<String> expected = multicastFrom(alive, "member");
    awaitDelivered(verifier, ids(alive), expected);
    quiet(verifier);
    verifier.checkSurvivors(survivors);
    verifier.checkReinserted(survivors, victim.id());
    for (Actor actor : alive)
      expectDeliveredOnce(verifier, actor.id(), expected);
  }

  /** The last two actors die and come back together. Every member must end with the same view. */
  private static void twoReturning(int actors) throws Exception {
    Started started = GroupManager.start(actors, 0, 150);
    Verifier verifier = started.verifier();
    final Actor first = started.actors().get(actors - 2);
    final Actor second = started.actors().get(actors - 1);
    List<String> survivors = new ArrayList<String>();
    for (int i = 0; i < actors - 2; i++)
      survivors.add(started.actors().get(i).id());

    // one after the other: two crashes at the very same time are a limit of the crash flush of Part 2
    kill(started, first, 20);
    awaitCrashed(verifier, first, actors - 1);
    kill(started, second, 20);
    awaitCrashed(verifier, second, actors - 2);
    comeBack(first);
    comeBack(second);

    final CountDownLatch go = new CountDownLatch(1);
    Thread a = new Thread(() -> { await(go); first.reinsert(); });
    Thread b = new Thread(() -> { await(go); second.reinsert(); });
    a.start();
    b.start();
    go.countDown();
    a.join();
    b.join();
    awaitReinserted(verifier, first);
    awaitReinserted(verifier, second);

    List<String> expected = multicastFrom(started.actors(), "two");
    awaitDelivered(verifier, ids(started.actors()), expected);
    quiet(verifier);
    verifier.checkSurvivors(survivors);
    verifier.checkReinserted(survivors, first.id());
    verifier.checkReinserted(survivors, second.id());
    for (Actor actor : started.actors())
      expectDeliveredOnce(verifier, actor.id(), expected);
  }

  /** Two actors come back together and one of them dies again. The other one must be reinserted. */
  private static void twoReturningOneDies(int delayMs) throws Exception {
    Started started = GroupManager.start(4, 0, 150);
    Verifier verifier = started.verifier();
    final Actor first = started.actors().get(2);
    final Actor second = started.actors().get(3);
    List<String> survivors = ids(started.actors().get(0), started.actors().get(1));

    kill(started, first, 20);
    awaitCrashed(verifier, first, 3);
    kill(started, second, 20);
    awaitCrashed(verifier, second, 2);
    comeBack(first);
    comeBack(second);

    final CountDownLatch go = new CountDownLatch(1);
    Thread a = new Thread(() -> { await(go); first.reinsert(); });
    Thread b = new Thread(() -> { await(go); second.reinsert(); });
    a.start();
    b.start();
    go.countDown();
    a.join();
    b.join();
    die(first, delayMs);
    awaitReinserted(verifier, second);

    List<Actor> alive = new ArrayList<Actor>();
    alive.add(started.actors().get(0));
    alive.add(started.actors().get(1));
    alive.add(second);
    List<String> expected = multicastFrom(alive, "one");
    awaitDelivered(verifier, ids(alive), expected);
    quiet(verifier);
    verifier.checkSurvivors(survivors);
    verifier.checkReinserted(survivors, second.id());
    for (Actor actor : alive)
      expectDeliveredOnce(verifier, actor.id(), expected);
  }

  /**
   * The victim is resurrected, then a member dies. The returning actor still holds a queue to the dead
   * member. It must not wait for it.
   */
  private static void staleConnectionMemberDied() throws Exception {
    Started started = GroupManager.start(3, 0, 150);
    Verifier verifier = started.verifier();
    Actor victim = started.actors().get(2);
    Actor member = started.actors().get(1);
    List<String> survivors = ids(started.actors().get(0));

    kill(started, victim, 20);
    awaitCrashed(verifier, victim, 2);
    comeBack(victim);
    die(member, 0);
    awaitCrashed(verifier, member, 1);

    victim.reinsert();
    awaitReinserted(verifier, victim);
    List<Actor> alive = new ArrayList<Actor>();
    alive.add(started.actors().get(0));
    alive.add(victim);
    List<String> expected = multicastFrom(alive, "stale1");
    awaitDelivered(verifier, ids(alive), expected);
    quiet(verifier);
    verifier.checkReinserted(survivors, victim.id());
    for (Actor actor : alive)
      expectDeliveredOnce(verifier, actor.id(), expected);
  }

  /**
   * The victim is resurrected, then a member dies and comes back (new life, new broker) and is reinserted
   * first. When the victim is reinserted, its old queue to that member is useless: it must reconnect.
   */
  private static void staleConnectionMemberCameBack() throws Exception {
    Started started = GroupManager.start(3, 0, 150);
    Verifier verifier = started.verifier();
    Actor victim = started.actors().get(2);
    Actor member = started.actors().get(1);
    List<String> survivors = ids(started.actors().get(0));

    kill(started, victim, 20);
    awaitCrashed(verifier, victim, 2);
    comeBack(victim);
    die(member, 0);
    awaitCrashed(verifier, member, 1);
    comeBack(member);
    member.reinsert();
    awaitReinserted(verifier, member);

    victim.reinsert();
    awaitReinserted(verifier, victim);
    List<String> expected = multicastFrom(started.actors(), "stale2");
    awaitDelivered(verifier, ids(started.actors()), expected);
    quiet(verifier);
    verifier.checkReinserted(survivors, member.id());
    verifier.checkReinserted(survivors, victim.id());
    for (Actor actor : started.actors())
      expectDeliveredOnce(verifier, actor.id(), expected);
  }

  // ------------------------------------------------------------------ helpers

  private static void kill(Started started, final Actor victim, int delayMs) {
    started.actors().get(0).task().post(new Runnable() {
      @Override
      public void run() {
        victim.task().kill();
      }
    }, delayMs);
  }

  private static void comeBack(Actor actor) throws InterruptedException {
    awaitState(actor, Actor.State.DEAD);
    actor.respawn();
    awaitState(actor, Actor.State.RESPAWNED);
  }

  private static void awaitCrashed(Verifier verifier, Actor victim, int reports) throws InterruptedException {
    if (!verifier.awaitCrashed(victim.id(), reports, TIMEOUT_MS))
      throw new AssertionError("the crash of " + victim.id() + " was not noticed by every survivor");
  }

  private static void awaitReinserted(Verifier verifier, Actor victim) throws InterruptedException {
    if (!verifier.awaitReinserted(victim.id(), TIMEOUT_MS))
      throw new AssertionError(victim.id() + " was not reinserted");
  }

  private static void quiet(Verifier verifier) throws InterruptedException {
    if (!verifier.awaitQuiet(500, 4 * TIMEOUT_MS))
      throw new AssertionError("deliveries never stopped");
  }

  /**
   * The actor dies between two events of its task, like a failure of the platform: what it sent in the
   * event it was running is sent. (A task killed in the middle of an event is not part of the model.)
   */
  private static void die(Actor actor, int delayMs) {
    final Task task = actor.task();
    Runnable kill = new Runnable() {
      @Override
      public void run() {
        task.kill();
      }
    };
    if (delayMs <= 0)
      task.post(kill);
    else
      task.post(kill, delayMs);
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static List<String> ids(Actor... actors) {
    List<String> ids = new ArrayList<String>();
    for (Actor actor : actors)
      ids.add(actor.id());
    return ids;
  }

  private static List<String> ids(List<Actor> actors) {
    List<String> ids = new ArrayList<String>();
    for (Actor actor : actors)
      ids.add(actor.id());
    return ids;
  }

  private static List<Actor> survivorActors(Started started, Actor victim) {
    List<Actor> result = new ArrayList<Actor>();
    for (Actor actor : started.actors())
      if (actor != victim)
        result.add(actor);
    return result;
  }

  /** Each given actor multicasts AFTER messages. Returns their payloads. */
  private static List<String> multicastFrom(List<Actor> actors, String tag) {
    List<String> payloads = new ArrayList<String>();
    for (Actor actor : actors) {
      for (int i = 0; i < AFTER; i++) {
        String payload = tag + "-" + actor.id() + "#" + i;
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

  private static void awaitState(Actor actor, Actor.State state) throws InterruptedException {
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    while (actor.state() != state) {
      if (System.currentTimeMillis() > deadline)
        throw new AssertionError(actor.id() + " did not reach " + state + " (state is " + actor.state() + ")");
      Thread.sleep(5);
    }
  }

  /** Waits until every listed actor has delivered every expected payload. */
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

