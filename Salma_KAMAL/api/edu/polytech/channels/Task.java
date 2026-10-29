package edu.polytech.channels;

/*
 * This is a task in the system, one active entity
 * that may send or receive bytes through one or more channels.
 * 
 * DO NOT EXTEND THIS CLASS.
 * DO NOT INVOKE THE METHOD "start()" on this thread.
 */
public abstract class Task extends Thread {

  /*
   * Returns the current task.
   */
	public static Task task() {
		return (Task) Thread.currentThread();
	}

  /*
   * Protected constructor.
   */
	protected Task(String name) {
		super(name);
	}

	/*
	 * Return the broker for this task.
   * Nota Bene: this method is thread safe,
   *            but it must be invoked on this task, 
   *            meaning on this thread.
	 */
	public abstract Broker getBroker();

	/* 
	 * This task is alive once it has started
	 * as a thread. So it is not yet alive when
	 * it is created.
	 * Nota Bene: this method is thread safe, it
	 *            can be invoked from any thread.
	 */
  public abstract boolean alive();

  /*
   * This task is dead once its thread has stopped.
   * Nota Bene: this method is thread safe, it
   *            can be invoked from any thread.
   */
  public abstract boolean dead();

  /*
   * Creates a new borker with the given name.
   * Nota Bene: invoke this method on this task, 
   *            meaning on this thread.
   *            this method is not thread-safe
   */
	public abstract Broker newBroker(String name);

  /*
   * Creates a new task with the given name,
   * the given broker, and the given runnable.
   * Nota Bene: invoke this method on this task, 
   *            meaning on this thread.
   *            this method is not thread-safe
   */
	public abstract Task newTask(Broker b, Runnable r, String name);

}
