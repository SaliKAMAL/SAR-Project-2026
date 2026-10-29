package edu.polytech.queues.tests;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import edu.polytech.queues.Bootstrap;
import edu.polytech.queues.Task;
import edu.polytech.queues.local.Boot;

/**
 * Checks the task runtime required by the SAR project:
 * a Lamport clock per task, force-kill that drops pending runnables,
 * and random failure limited to tasks marked as failable.
 *
 * Run from the Eclipse project directory:
 *   javac -d bin $(find api local utils.queues tests -name '*.java')
 *   java -cp bin edu.polytech.queues.tests.TestLogicalClock
 */
public class TestLogicalClock {

  public static void main(String[] args) throws Exception {
    Task.setFailureProbability(0);
    Bootstrap bootstrap = new Boot();
    try {
      clockTicksOncePerExecutedRunnable(bootstrap);
      witnessMovesTheClockForward(bootstrap);
      killDropsPendingAndDelayedRunnables(bootstrap);
      killFromAnotherTaskDoesNotTouchOthers(bootstrap);
      killCascadesToTasksCreatedFromTheVictim(bootstrap);
      randomFailureKillsOnlyFailableTasks(bootstrap);
      nonFailableTaskSurvivesCertainFailure(bootstrap);
    } finally {
      Task.setFailureProbability(0);
    }
    System.out.println("logical clock tests: PASS");
    System.exit(0);
  }

  private static void clockTicksOncePerExecutedRunnable(Bootstrap bootstrap) throws Exception {
    Task task = bootstrap.newTask(null, "clock");
    long[] seen = new long[3];
    CountDownLatch done = new CountDownLatch(1);
    task.post(() -> seen[0] = task.logicalClock());
    task.post(() -> seen[1] = task.logicalClock());
    task.post(() -> {
      seen[2] = task.logicalClock();
      done.countDown();
    });
    await(done);
    check(seen[0] == 1 && seen[1] == 2 && seen[2] == 3, "clock values " + seen[0] + "," + seen[1] + "," + seen[2]);
    check(task.logicalClock() == 3, "clock after three runnables");
  }

  private static void witnessMovesTheClockForward(Bootstrap bootstrap) throws Exception {
    Task task = bootstrap.newTask(null, "witness");
    long[] seen = new long[2];
    CountDownLatch done = new CountDownLatch(1);
    task.post(() -> {
      check(task.logicalClock() == 1, "tick before witness");
      task.witness(10);
      task.witness(4);
      seen[0] = task.logicalClock();
    });
    task.post(() -> {
      seen[1] = task.logicalClock();
      done.countDown();
    });
    await(done);
    check(seen[0] == 10, "witness keeps the greater timestamp");
    check(seen[1] == 11, "next runnable ticks after witness");
  }

  private static void killDropsPendingAndDelayedRunnables(Bootstrap bootstrap) throws Exception {
    Task task = bootstrap.newTask(null, "suicide");
    AtomicInteger ran = new AtomicInteger();
    CountDownLatch started = new CountDownLatch(1);
    task.post(() -> {
      task.post(() -> ran.incrementAndGet());
      task.post(() -> ran.incrementAndGet(), 40);
      task.kill();
      task.post(() -> ran.incrementAndGet());
      started.countDown();
    });
    await(started);
    Thread.sleep(80);
    check(ran.get() == 0, "pending runnable executed after kill");
    check(task.dead(), "killed task is not dead");
    check(task.logicalClock() == 1, "dropped runnables must not tick the clock");
  }

  private static void killFromAnotherTaskDoesNotTouchOthers(Bootstrap bootstrap) throws Exception {
    Task killer = bootstrap.newTask(null, "killer");
    Task victim = bootstrap.newTask(null, "victim");
    Task other = bootstrap.newTask(null, "other");
    AtomicInteger victimRan = new AtomicInteger();
    AtomicInteger otherRan = new AtomicInteger();
    CountDownLatch done = new CountDownLatch(1);
    killer.post(() -> {
      victim.post(() -> victimRan.incrementAndGet());
      other.post(() -> {
        otherRan.incrementAndGet();
        done.countDown();
      });
      victim.kill();
    });
    await(done);
    check(victimRan.get() == 0, "victim runnable was not dropped");
    check(otherRan.get() == 1, "unrelated task was dropped");
    check(victim.dead() && victim.logicalClock() == 0, "victim clock");
    check(!other.dead(), "other task should stay alive");
  }

  private static void killCascadesToTasksCreatedFromTheVictim(Bootstrap bootstrap) throws Exception {
    Task parent = bootstrap.newTask(null, "parent");
    Task child = parent.newTask("child");
    Task watcher = bootstrap.newTask(null, "watcher");
    AtomicInteger childRan = new AtomicInteger();
    CountDownLatch done = new CountDownLatch(1);
    parent.post(() -> {
      child.post(() -> childRan.incrementAndGet());
      parent.kill();
    });
    watcher.post(() -> done.countDown());
    await(done);
    Thread.sleep(40);
    check(parent.dead(), "parent not dead");
    check(child.dead(), "child task was not killed with its parent");
    check(childRan.get() == 0, "child runnable ran after the parent was killed");
    check(child.logicalClock() == 0, "child clock ticked after cascade kill");
  }

  private static void randomFailureKillsOnlyFailableTasks(Bootstrap bootstrap) throws Exception {
    Task.setFailureProbability(1);
    Task victim = bootstrap.newTask(null, "random-victim");
    Task watcher = bootstrap.newTask(null, "random-watcher");
    victim.setMayFail(true);
    AtomicInteger ran = new AtomicInteger();
    CountDownLatch done = new CountDownLatch(1);
    victim.post(() -> ran.incrementAndGet());
    watcher.post(() -> done.countDown());
    await(done);
    check(ran.get() == 0, "failable task ran despite a certain random failure");
    check(victim.dead(), "random failure did not kill the task");
    check(victim.logicalClock() == 0, "a runnable killed before it starts must not tick");
    check(!watcher.dead() && watcher.logicalClock() == 1, "non-failable watcher was killed");
    Task.setFailureProbability(0);
  }

  private static void nonFailableTaskSurvivesCertainFailure(Bootstrap bootstrap) throws Exception {
    Task.setFailureProbability(1);
    Task safe = bootstrap.newTask(null, "internal");
    CountDownLatch done = new CountDownLatch(1);
    AtomicInteger ran = new AtomicInteger();
    safe.post(() -> ran.incrementAndGet());
    safe.post(() -> done.countDown());
    await(done);
    check(!safe.mayFail(), "tasks must not be failable by default");
    check(ran.get() == 1 && safe.logicalClock() == 2, "internal task was randomly killed");
    check(!safe.dead(), "internal task died");
    Task.setFailureProbability(0);
  }

  private static void await(CountDownLatch latch) throws InterruptedException {
    if (!latch.await(3, TimeUnit.SECONDS))
      throw new AssertionError("timed out waiting for the task runtime");
  }

  private static void check(boolean condition, String message) {
    if (!condition)
      throw new AssertionError(message);
  }

}
