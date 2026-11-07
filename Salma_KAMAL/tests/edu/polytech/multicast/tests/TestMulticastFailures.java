package edu.polytech.multicast.tests;

import java.util.ArrayList;
import java.util.List;

import edu.polytech.multicast.Actor;
import edu.polytech.multicast.GroupManager;
import edu.polytech.multicast.GroupManager.Started;
import edu.polytech.queues.Task;

/**
 * Tests fail-stop crashes and the shrinking group flush mechanism.
 * Run from the Eclipse project directory:
 *   javac -d bin $(find api local utils.queues tests -name '*.java')
 *   java -cp bin edu.polytech.multicast.tests.TestMulticastFailures
 */
public class TestMulticastFailures {

  private static final int ACTORS = 3;

  public static void main(String[] args) throws Exception {
    try {
      System.out.println("--- Running Forced Suicide (period = 100ms) ---");
      runForcedSuicide(100);
      System.out.println("--- Running Forced Suicide (Burst mode: period = 0ms) ---");
      runForcedSuicide(0);

      System.out.println("--- Running Random Failures (period = 100ms) ---");
      runRandomFailure(100);
      System.out.println("--- Running Random Failures (Burst mode: period = 0ms) ---");
      runRandomFailure(0);

      System.out.println("--- Running Simultaneous Crashes (period = 100ms) ---");
      runSimultaneousCrashes(100);

      System.out.println("--- Running Verifier Strictness Test ---");
      testVerifierStrictness();

    } catch (Throwable failure) {
      failure.printStackTrace(System.err);
      System.exit(1);
    }
    System.out.println("multicast failures tests: PASS");
    System.exit(0);
  }

  private static void runForcedSuicide(int period) throws Exception {
    Started started = GroupManager.start(ACTORS, period);
    final Actor victim = started.actors().get(ACTORS - 1); // The last actor is the victim

    // Terminate the actor after a short delay to allow some messages to be exchanged
    started.actors().get(0).task().post(new Runnable() {
      @Override
      public void run() {
        victim.task().kill();
      }
    }, 50);

    // Wait for the system to stabilize, perform the flush, and finish delivering pending messages
    Thread.sleep(3000); 
    validateSurvivors(started);
  }

  private static void runRandomFailure(int period) throws Exception {
    Task.setFailureProbability(0.4); // 40% chance of crashing during each execution
    Started started = GroupManager.start(ACTORS, period);
    
    // Make the last actor susceptible to random failures 
    Actor victim = started.actors().get(ACTORS - 1);
    victim.task().setMayFail(true);

    Thread.sleep(3000); 
    validateSurvivors(started);
    
    Task.setFailureProbability(0); // Reset the configuration for the remaining tests
  }

 private static void runSimultaneousCrashes(int period) throws Exception {
    // Run the same test with 4 actors.
    Started started = GroupManager.start(4, period); 
    final Actor victim1 = started.actors().get(2);
    final Actor victim2 = started.actors().get(3);

    // Schedule a simultaneous double kill
    started.actors().get(0).task().post(new Runnable() {
      @Override
      public void run() {
        victim1.task().kill();
        victim2.task().kill();
      }
    }, 50);

    Thread.sleep(3000); 
    validateSurvivors(started);
  } 

  private static void validateSurvivors(Started started) {
    List<String> survivors = new ArrayList<String>();
    for (Actor a : started.actors()) {
      // An actor is considered a survivor if it has not been declared dead by the flush operation
      if (!started.verifier().getCrashedActors().contains(a.id())) {
        survivors.add(a.id());
      }
    }
	    
    System.out.println("Verified: " + survivors.size() + " survivors remaining.");
	    
    // The real crash test: compare the message sequences of all surviving actors
    started.verifier().checkSurvivors(survivors);
  }

  private static void testVerifierStrictness() throws Exception {
    Started started = GroupManager.start(2, 100);
    Thread.sleep(2000); // Wait for the 2 actors to finish normally

    // Create a fake list of survivors with an imposter !
    List<String> fakeSurvivors = new ArrayList<String>();
    fakeSurvivors.add(started.actors().get(0).id()); // A real actor
    fakeSurvivors.add("g-imposteur"); // The imposter (0 messages delivered)

    try {
      // Submit the tampered list to the verifier
      started.verifier().checkSurvivors(fakeSurvivors);
      
      // If execution reaches this line, the verifier failed to detect the issue (Problem 4 is still present)
      throw new RuntimeException("ECHEC: Le Verifier a accepté un acteur avec 0 message !");
      
    } catch (AssertionError e) {
      // Success: the verifier correctly detected the issue and raised an error.
      System.out.println("Verified: Verifier strictness proven! Caught expected error -> " + e.getMessage());
    }
  }

}
