package edu.polytech.queues;

/**
 * Cause reported when a task is force-killed, either by {@link Task#kill()}
 * or by a random failure inside the runtime.
 */
public class TaskKilled extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public TaskKilled(String taskName) {
    super("task killed: " + taskName);
  }

}
