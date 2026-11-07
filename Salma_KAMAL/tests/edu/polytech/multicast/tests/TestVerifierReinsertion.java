package edu.polytech.multicast.tests;
import java.util.Arrays;
import java.util.List;

import edu.polytech.multicast.Verifier;

/**
 * Tests the oracle itself: the verifier must accept a correct history and reject wrong ones.
 * No actor is started, so this test does not depend on the protocol.
 * Run from the Eclipse project directory:
 *   javac -d bin $(find api local utils.queues tests -name '*.java')
 *   java -cp bin edu.polytech.multicast.tests.TestVerifierReinsertion
 */
public class TestVerifierReinsertion {

  private static final List<String> SURVIVORS = Arrays.asList("A", "B");

  private interface Action {
    void run() throws Exception;
  }

  public static void main(String[] args) {
    try {
      acceptsGoodHistory();
      rejectsAbsentId();
      rejectsSequenceThatIsNotTheEnd();
      rejectsEmptyLife();
      rejectsDuplicate();
      rejectsReinsertedWithoutResurrected();
      awaitReinsertedTimesOutThenSucceeds();
      countsCrashReports();
      acceptsTwoLives();
    } catch (Throwable failure) {
      failure.printStackTrace(System.err);
      System.exit(1);
    }
    System.out.println("verifier reinsertion tests: PASS");
    System.exit(0);
  }

  private static void deliver(Verifier v, String actor, long id) {
    v.delivered(actor, id, ("m" + id).getBytes());
  }

  private static void deliverAll(Verifier v, String actor, long... ids) {
    for (long id : ids)
      deliver(v, actor, id);
  }

  /** A and B deliver 1..4, R delivered 1 then died, R comes back, then everybody delivers 5 and 6. */
  private static Verifier historyUntilReinsertion() {
    Verifier v = new Verifier();
    deliver(v, "R", 1);
    deliverAll(v, "A", 1, 2, 3, 4);
    deliverAll(v, "B", 1, 2, 3, 4);
    v.crashed("R");
    v.crashed("R");
    v.resurrected("R");
    v.reinserted("R");
    return v;
  }

  private static void acceptsGoodHistory() {
    Verifier v = historyUntilReinsertion();
    for (String actor : Arrays.asList("A", "B", "R"))
      deliverAll(v, actor, 5, 6);
    v.checkSurvivors(SURVIVORS);
    v.checkReinserted(SURVIVORS, "R");
  }

  private static void rejectsAbsentId() {
    Verifier v = historyUntilReinsertion();
    deliverAll(v, "A", 5, 6);
    deliverAll(v, "B", 5, 6);
    deliverAll(v, "R", 4, 5, 6); // 4 was delivered by A and B while R was away
    expectAssertion("absent id", () -> v.checkReinserted(SURVIVORS, "R"));
  }

  private static void rejectsSequenceThatIsNotTheEnd() {
    Verifier v = historyUntilReinsertion();
    deliverAll(v, "A", 5, 6);
    deliverAll(v, "B", 5, 6);
    deliverAll(v, "R", 5); // R missed 6
    expectAssertion("missing last message", () -> v.checkReinserted(SURVIVORS, "R"));
  }

  private static void rejectsEmptyLife() {
    Verifier v = historyUntilReinsertion();
    deliverAll(v, "A", 5, 6);
    deliverAll(v, "B", 5, 6);
    expectAssertion("empty life", () -> v.checkReinserted(SURVIVORS, "R"));
  }

  private static void rejectsDuplicate() {
    Verifier v = historyUntilReinsertion();
    deliverAll(v, "A", 5, 6);
    deliverAll(v, "B", 5, 6);
    deliverAll(v, "R", 5, 5, 6);
    expectAssertion("duplicate", () -> v.checkReinserted(SURVIVORS, "R"));
  }

  private static void rejectsReinsertedWithoutResurrected() {
    final Verifier v = new Verifier();
    expect(IllegalStateException.class, "reinserted without resurrected", () -> v.reinserted("R"));
  }

  private static void awaitReinsertedTimesOutThenSucceeds() throws Exception {
    Verifier v = new Verifier();
    v.resurrected("R");
    if (v.awaitReinserted("R", 50))
      throw new AssertionError("awaitReinserted returned true before reinserted");
    v.reinserted("R");
    if (!v.awaitReinserted("R", 50))
      throw new AssertionError("awaitReinserted returned false after reinserted");
  }

  private static void countsCrashReports() throws Exception {
    Verifier v = new Verifier();
    v.crashed("R");
    v.crashed("R");
    if (!v.awaitCrashed("R", 2, 50))
      throw new AssertionError("two reports expected");
    if (v.awaitCrashed("R", 3, 50))
      throw new AssertionError("third report was not sent");
  }

  /** R dies and comes back twice. Only the last life is checked, against the last absent list. */
  private static void acceptsTwoLives() {
    Verifier v = historyUntilReinsertion();
    for (String actor : Arrays.asList("A", "B", "R"))
      deliverAll(v, actor, 5, 6);
    v.crashed("R");
    deliverAll(v, "A", 7);
    deliverAll(v, "B", 7);
    v.resurrected("R");
    v.reinserted("R");
    for (String actor : Arrays.asList("A", "B", "R"))
      deliverAll(v, actor, 8, 9);
    v.checkSurvivors(SURVIVORS);
    v.checkReinserted(SURVIVORS, "R");
  }

  private static void expectAssertion(String what, Action action) {
    expect(AssertionError.class, what, action);
  }

  private static void expect(Class<? extends Throwable> type, String what, Action action) {
    try {
      action.run();
    } catch (Throwable t) {
      if (type.isInstance(t))
        return;
      throw new AssertionError(what + ": expected " + type.getSimpleName() + " but got " + t);
    }
    throw new AssertionError(what + ": expected " + type.getSimpleName() + " but nothing was thrown");
  }
}

