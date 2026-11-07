package edu.polytech.queues;

import edu.polytech.utils.Executor;

public abstract class Task {

  public abstract String getName();
  public abstract QueueBroker getBroker();
  public abstract void post(Runnable r);
  public abstract void post(Runnable r, int delay);
  
  public abstract boolean dead();
    
  public abstract Task newTask(String name);
  public abstract QueueBroker newBroker(String name);
  
  public static Task task() {
    return Executor.task();
  }
  
  public interface Listener {
    public void failed(Task t, Throwable th);    
    public void completed(Task t,Object o);    
  }
  public abstract void set(Listener l);
  public abstract void exit(Object o);
  public abstract void fail(Throwable th);

  /**
   * Lamport clock of this task. It ticks once each time a runnable posted on
   * this task actually starts. Dropped runnables do not tick.
   */
  public abstract long logicalClock();

  /**
   * Observe a timestamp coming from another task.
   * The clock becomes max(local clock, timestamp).
   */
  public abstract void witness(long timestamp);

  /**
   * When false (the default), the runtime will never kill this task at random.
   * Internal tasks must stay non-failable.
   */
  public abstract void setMayFail(boolean mayFail);

  public abstract boolean mayFail();

  /**
   * Force-stop this task and every task created from it.
   * Pending and delayed runnables are dropped and will never execute.
   */
  public abstract void kill();

  /**
   * Probability in [0, 1] that a failable task is killed instead of running
   * its next runnable. Zero disables random failures.
   */
  public static void setFailureProbability(double probability) {
    Executor.setFailureProbability(probability);
  }

  public static double failureProbability() {
    return Executor.failureProbability();
  }

}
