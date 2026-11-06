package edu.polytech.queues.local;

import edu.polytech.queues.Bootstrap;
import edu.polytech.queues.Task;
import edu.polytech.utils.Executor;

public class Boot implements Bootstrap {

  public Boot() {
  }

  @Override
  public Task newTask(Runnable r, String name) {
    Task task = Executor.self().newTask(name);
    if (r != null)
      task.post(r);
    return task;
  }

}
