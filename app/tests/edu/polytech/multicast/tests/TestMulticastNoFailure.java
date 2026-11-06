package edu.polytech.multicast.tests;

import edu.polytech.multicast.GroupManager;
import edu.polytech.multicast.GroupManager.Started;

/**
 * Totally-ordered multicast with every actor still alive.
 * Run from the Eclipse project directory:
 *   javac -d bin $(find api local utils.queues tests -name '*.java')
 *   java -cp bin edu.polytech.multicast.tests.TestMulticastNoFailure
 */
public class TestMulticastNoFailure {

  private static final int ACTORS = 3;
  private static final long TIMEOUT_MS = 15000L;

  public static void main(String[] args) throws Exception {
    try {
      run(200);
      run(0);
    } catch (Throwable failure) {
      failure.printStackTrace(System.err);
      System.exit(1);
    }
    System.out.println("multicast no failure tests: PASS");
    System.exit(0);
  }

  private static void run(int period) throws Exception {
    Started started = GroupManager.start(ACTORS, period);
    boolean complete = started.verifier().awaitComplete(ACTORS, GroupManager.MESSAGES_PER_ACTOR, TIMEOUT_MS);
    if (!complete)
      throw new AssertionError("period " + period + " did not deliver every message");
    started.verifier().check();
  }
}
