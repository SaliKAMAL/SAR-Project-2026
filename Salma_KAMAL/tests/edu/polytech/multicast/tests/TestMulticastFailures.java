package edu.polytech.multicast.tests;

import edu.polytech.multicast.Actor;
import edu.polytech.multicast.GroupManager;
import edu.polytech.multicast.GroupManager.Started;
import edu.polytech.queues.Task;
import java.util.ArrayList;
import java.util.List;

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

    } catch (Throwable failure) {
      failure.printStackTrace(System.err);
      System.exit(1);
    }
    System.out.println("multicast failures tests: PASS");
    System.exit(0);
  }

  private static void runForcedSuicide(int period) throws Exception {
    Started started = GroupManager.start(ACTORS, period);
    final Actor victim = started.actors().get(ACTORS - 1); // Le dernier acteur sera la victime

    // On tue l'acteur après un court délai pour laisser quelques messages s'échanger
    started.actors().get(0).task().post(new Runnable() {
      @Override
      public void run() {
        victim.task().kill();
      }
    }, 50);

    // On attend que le système se stabilise, déclenche le Flush, et finisse la délivrance
    Thread.sleep(3000); 
    validateSurvivors(started);
  }

  private static void runRandomFailure(int period) throws Exception {
    Task.setFailureProbability(0.4); // 40% de chance de mourir à chaque exécution
    Started started = GroupManager.start(ACTORS, period);
    
    // On rend le dernier acteur vulnérable aux pannes aléatoires
    Actor victim = started.actors().get(ACTORS - 1);
    victim.task().setMayFail(true);

    Thread.sleep(3000); 
    validateSurvivors(started);
    
    Task.setFailureProbability(0); // Réinitialisation pour les autres tests
  }

  private static void validateSurvivors(Started started) {
    List<String> survivors = new ArrayList<String>();
    for (Actor a : started.actors()) {
      // Un acteur est un survivant s'il n'a pas été déclaré mort par notre Flush
      if (!started.verifier().getCrashedActors().contains(a.id())) {
        survivors.add(a.id());
      }
    }
	    
    System.out.println("Verified: " + survivors.size() + " survivors remaining.");
	    
    // Le vrai crash-test : on compare les séquences des survivants !
    started.verifier().checkSurvivors(survivors);
  }
}
