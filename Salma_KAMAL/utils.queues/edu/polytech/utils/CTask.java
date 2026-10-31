package edu.polytech.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import edu.polytech.queues.QueueBroker;
import edu.polytech.queues.Task;
import edu.polytech.queues.local.CMessageQueue;
import edu.polytech.queues.local.CQueueBroker;

public class CTask extends Task {
  Executor e;
  QueueBroker broker;
  String name;
  volatile boolean failed, completed;
  volatile boolean mayFail;
  volatile long clock;
  Listener l;
  Task tl;
  Object result;
  Throwable cause;
  private final List<CTask> children = new ArrayList<>();

  CTask(String name) {
    this.name = name;
    this.e = Executor.self();
  }

  @Override
  public String toString() {
    return "Task " + name;
  }

  public void set(QueueBroker qb) {
    if (broker != null)
      throw new IllegalStateException("Task " + name + " has a broker already.");
    broker = qb;
  }

  @Override
  public void post(Runnable r) {
    if (!dead())
      e.post(this, r);
  }

  @Override
  public void post(Runnable r, int delay) {
    if (!dead())
      e.post(this, r, delay);
  }

  @Override
  public QueueBroker getBroker() {
    return broker;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean dead() {
    return failed || completed;
  }

  @Override
  public Task newTask(String name) {
    CTask child = new CTask(name);
    boolean killNow;
    synchronized (e) {
      children.add(child);
      killNow = dead();
    }
    if (killNow)
      child.kill();
    return child;
  }

  @Override
  public QueueBroker newBroker(String name) {
    return new CQueueBroker(name);
  }

  @Override
  public void set(Listener l) {
    if (failed || completed)
      throw new IllegalStateException("Dead task");
    this.tl = Task.task();
    this.l = l;
  }

  @Override
  public void fail(Throwable th) {
    e.fail(this, th);
  }

  @Override
  public void exit(Object o) {
    e.complete(this, o);
  }

  @Override
  public long logicalClock() {
    return clock;
  }

  @Override
  public void witness(long timestamp) {
    if (timestamp > clock)
      clock = timestamp;
  }

  void tick() {
    clock = clock + 1;
  }

  @Override
  public void setMayFail(boolean mayFail) {
    this.mayFail = mayFail;
  }

  @Override
  public boolean mayFail() {
    return mayFail;
  }

  @Override
  public void kill() {
    e.kill(this);
  }

  void markFailed(Throwable th) {
    failed = true;
    cause = th;
  }

  void markCompleted(Object o) {
    completed = true;
    result = o;
  }

  boolean alreadyFailed() {
    return failed;
  }

  List<CTask> takeChildren() {
    List<CTask> copy = new ArrayList<>(children);
    children.clear();
    return copy;
  }

  /*
   * Really important to have this method because it is possible that a runnable
   * on this task invoked exit, but failed afterwards.
   */
  void done() {
    Iterator<CMessageQueue> i = queues.iterator();
    while (i.hasNext()) {
      CMessageQueue mq = i.next();
      mq.close();
    }
    Listener listener = this.l;
    if (listener != null) {
      tl.post(() -> {
        if (failed)
          listener.failed(this, cause);
        else
          listener.completed(this, result);
      });
    } else if (failed && !(cause instanceof edu.polytech.queues.TaskKilled)) {
      Executor.log("Failed task " + name, cause);
    }
  }

  List<CMessageQueue> queues = new LinkedList<>();

  void register(CMessageQueue mq) {
    queues.add(mq);
  }
}
