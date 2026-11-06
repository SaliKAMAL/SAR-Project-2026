package edu.polytech.channels;

/*
 * Each implementation will offer a "bootstrap class",
 * with a public constructor without arguments.
 * This class must be instantiated once and the instance
 * will enable the creation of the first brokers and tasks.
 * 
 * Once the runtime is bootstrapped, this instance should not
 * be used any longer, one should use the methods on the class Task
 * to create new brokers and new tasks.
 */
public interface Bootstrap {
  Broker newBroker(String name);
  Task newTask(Broker b, Runnable r, String name);
}
