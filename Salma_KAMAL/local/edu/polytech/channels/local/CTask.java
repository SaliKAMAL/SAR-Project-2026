package edu.polytech.channels.local;

import edu.polytech.channels.Broker;
import edu.polytech.channels.Task;

public class CTask extends Task {

  protected Broker broker;
  protected Runnable boot;
  protected volatile boolean alive;
  protected volatile boolean dead;

  public CTask(Broker b, Runnable r, String name) {
    super(name);
    broker = b;
    boot = r;
    start();
  }

  @Override
  public Broker getBroker() {
    assert(this==Thread.currentThread());
    return broker;
  }
  
  @Override
  public boolean alive() {
    return alive;
  }

  @Override
  public boolean dead() {
    return dead;
  }

  @Override
  public Broker newBroker(String name) {
    assert(this==Thread.currentThread());
    return new CBroker(name);
  }

  @Override
  public Task newTask(Broker b, Runnable r, String n) {
    assert(this==Thread.currentThread());
    CTask task = new CTask(b, r, n);
    return task;
  }

  @Override
  public void start() {
    assert(this==Thread.currentThread());
    assert(!alive && !dead);
    alive = true;
    super.start();
  }

  @Override
  public final void run() {
    try {
      boot.run();
    } catch (Throwable th) {
      System.err.println(th.getMessage());
      th.printStackTrace(System.err);
    } finally {
      alive = false;
      dead = true;
    }
  }

}
