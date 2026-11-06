package edu.polytech.queues.local;

import java.util.ArrayDeque;
import java.util.Arrays;

import edu.polytech.queues.MessageQueue;
import edu.polytech.queues.QueueBroker;
import edu.polytech.queues.Task;
import edu.polytech.utils.CTask;
import edu.polytech.utils.Executor;

public class CMessageQueue implements MessageQueue {

  private enum Mode {
    OPEN,
    CLOSING,
    CLOSED
  }

  private final CQueueBroker owner;
  private CMessageQueue peer;

  private Listener listener;
  private CTask listenerTask;
  private final ArrayDeque<byte[]> incoming = new ArrayDeque<>();

  private Mode mode = Mode.OPEN;
  private boolean closeNotified;
  private boolean drainPosted;

  CMessageQueue(CQueueBroker owner) {
    this.owner = owner;
    Executor.self().register(owner.getTask(), this);
  }

  void setPeer(CMessageQueue peer) {
    this.peer = peer;
  }

  @Override
  public QueueBroker broker() {
    return owner;
  }

  @Override
  public void setListener(Listener listener) {
    Task current = Task.task();
    if (!(current instanceof CTask))
      throw new IllegalStateException("setListener must be called on a task");
    this.listener = listener;
    this.listenerTask = (CTask) current;
    scheduleDrain();
  }

  @Override
  public boolean send(byte[] bytes, int offset, int length, SendListener listener) {
    if (bytes == null || offset < 0 || length < 0 || (long) offset + length > bytes.length)
      throw new IllegalArgumentException("invalid message range");
    Task current = Task.task();
    if (!(current instanceof CTask))
      throw new IllegalStateException("send must be called on a task");
    CTask sender = (CTask) current;
    if (mode != Mode.OPEN) {
      returnOwnership(sender, listener, bytes, offset, length);
      return false;
    }
    byte[] message = Arrays.copyOfRange(bytes, offset, offset + length);
    peer.enqueue(message);
    returnOwnership(sender, listener, bytes, offset, length);
    return true;
  }

  @Override
  public void close() {
    if (mode == Mode.CLOSED)
      return;
    boolean tellPeer = mode == Mode.OPEN;
    mode = Mode.CLOSED;
    incoming.clear();
    if (tellPeer && peer != null)
      peer.peerClosed();
    notifyClosed();
  }

  @Override
  public boolean closed() {
    return mode == Mode.CLOSED;
  }

  private void enqueue(byte[] message) {
    if (mode == Mode.CLOSED)
      return;
    incoming.addLast(message);
    scheduleDrain();
  }

  private void peerClosed() {
    if (mode == Mode.CLOSED)
      return;
    mode = Mode.CLOSING;
    scheduleDrain();
  }

  private void scheduleDrain() {
    if (listener == null || listenerTask == null || drainPosted)
      return;
    drainPosted = true;
    listenerTask.post(() -> {
      drainPosted = false;
      drain();
    });
  }

  private void drain() {
    if (listener == null || listenerTask == null)
      return;
    if (mode == Mode.CLOSED) {
      notifyClosed();
      return;
    }
    if (!incoming.isEmpty()) {
      byte[] message = incoming.removeFirst();
      listener.received(message);
    }
    if (mode == Mode.CLOSED) {
      notifyClosed();
      return;
    }
    if (!incoming.isEmpty() && listener != null) {
      scheduleDrain();
      return;
    }
    if (mode == Mode.CLOSING && incoming.isEmpty()) {
      mode = Mode.CLOSED;
      notifyClosed();
    }
  }

  private void notifyClosed() {
    if (closeNotified || listener == null || listenerTask == null)
      return;
    closeNotified = true;
    Listener closedListener = listener;
    listenerTask.post(closedListener::closed);
  }

  private static void returnOwnership(CTask sender, SendListener listener, byte[] bytes, int offset, int length) {
    if (listener == null)
      return;
    sender.post(() -> listener.sent(bytes, offset, length));
  }

}
