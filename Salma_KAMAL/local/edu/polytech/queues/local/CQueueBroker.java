package edu.polytech.queues.local;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

import edu.polytech.queues.QueueBroker;
import edu.polytech.queues.Task;
import edu.polytech.utils.CTask;
import edu.polytech.utils.Executor;

public class CQueueBroker implements QueueBroker {

  private static final Map<String, CQueueBroker> REGISTRY = new HashMap<>();

  private final String name;
  private final CTask task;
  private final Map<Integer, BindListener> bindings = new HashMap<>();
  private final Map<Integer, ArrayDeque<PendingConnect>> pending = new HashMap<>();

  public CQueueBroker(String name) {
    Task current = Task.task();
    if (!(current instanceof CTask))
      throw new IllegalStateException("QueueBroker must be created on a task");
    if (name == null)
      throw new IllegalArgumentException("name");
    this.task = (CTask) current;
    this.name = name;
    synchronized (REGISTRY) {
      if (REGISTRY.containsKey(name))
        throw new IllegalArgumentException("Broker already exists: " + name);
      REGISTRY.put(name, this);
    }
    if (task.getBroker() == null)
      Executor.self().set(task, this);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Task getTask() {
    return task;
  }

  @Override
  public synchronized boolean bind(int port, BindListener listener) {
    if (listener == null)
      throw new IllegalArgumentException("listener");
    if (bindings.containsKey(port))
      return false;
    bindings.put(port, listener);
    ArrayDeque<PendingConnect> waiting = pending.remove(port);
    if (waiting != null) {
      for (PendingConnect connect : waiting)
        open(connect.clientBroker, listener, connect.listener);
    }
    return true;
  }

  @Override
  public synchronized boolean unbind(int port) {
    BindListener listener = bindings.remove(port);
    if (listener == null)
      return false;
    task.post(listener::unbound);
    ArrayDeque<PendingConnect> waiting = pending.remove(port);
    if (waiting != null) {
      for (PendingConnect connect : waiting)
        connect.clientBroker.task.post(connect.listener::refused);
    }
    return true;
  }

  @Override
  public boolean connect(String name, int port, ConnectListener listener) {
    if (listener == null)
      throw new IllegalArgumentException("listener");
    CQueueBroker remote;
    synchronized (REGISTRY) {
      remote = REGISTRY.get(name);
    }
    if (remote == null)
      return false;
    remote.acceptConnect(this, port, listener);
    return true;
  }

  private synchronized void acceptConnect(CQueueBroker clientBroker, int port, ConnectListener listener) {
    BindListener bindListener = bindings.get(port);
    if (bindListener == null) {
      pending.computeIfAbsent(port, key -> new ArrayDeque<>())
          .add(new PendingConnect(clientBroker, listener));
      return;
    }
    open(clientBroker, bindListener, listener);
  }

  private void open(CQueueBroker clientBroker, BindListener bindListener, ConnectListener connectListener) {
    CMessageQueue serverQueue = new CMessageQueue(this);
    CMessageQueue clientQueue = new CMessageQueue(clientBroker);
    serverQueue.setPeer(clientQueue);
    clientQueue.setPeer(serverQueue);
    task.post(() -> bindListener.accepted(serverQueue));
    clientBroker.task.post(() -> connectListener.connected(clientQueue));
  }

  private static final class PendingConnect {
    final CQueueBroker clientBroker;
    final ConnectListener listener;

    PendingConnect(CQueueBroker clientBroker, ConnectListener listener) {
      this.clientBroker = clientBroker;
      this.listener = listener;
    }
  }

}
