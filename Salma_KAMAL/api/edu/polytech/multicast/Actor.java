package edu.polytech.multicast;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import edu.polytech.queues.MessageQueue;
import edu.polytech.queues.QueueBroker;
import edu.polytech.queues.QueueBroker.BindListener;
import edu.polytech.queues.QueueBroker.ConnectListener;
import edu.polytech.queues.Task;

/**
 * One group member. It owns a single task and talks to the others only through
 * message queues. In this part the member never leaves and never crashes.
 */
public final class Actor {

  private final String id;
  private final int index;
  private final Task task;
  private final Group group;
  private final Verifier verifier;
  private final Actor[] members;
  private final int period;
  private final int messagesToSend;
  private final Map<String, MessageQueue> peers = new HashMap<String, MessageQueue>();
  private final Map<String, Long> lastSeen = new HashMap<String, Long>();
  private final PriorityQueue<MulticastMessage> pending = new PriorityQueue<MulticastMessage>();
  private final Set<Long> deliveredIds = new HashSet<Long>();
  private QueueBroker broker;
  private int sent;
  private Runnable onLinked;

  Actor(String id, int index, Task task, Group group, Verifier verifier, Actor[] members, int period,
      int messagesToSend) {
    this.id = id;
    this.index = index;
    this.task = task;
    this.group = group;
    this.verifier = verifier;
    this.members = members;
    this.period = period;
    this.messagesToSend = messagesToSend;
  }

  public String id() {
    return id;
  }

  public Task task() {
    return task;
  }

  void openBroker() {
    broker = task.newBroker(id);
  }

  void bindToHigherPeers() {
    for (int other = index + 1; other < members.length; other++) {
      final int port = other;
      final String peerId = members[other].id;
      boolean bound = broker.bind(port, new BindListener() {
        @Override
        public void accepted(MessageQueue queue) {
          link(peerId, queue);
        }

        @Override
        public void unbound() {
        }
      });
      if (!bound)
        throw new IllegalStateException(id + " could not bind port " + port);
    }
  }

  void connectToLowerPeers() {
    for (int other = 0; other < index; other++) {
      final String peerId = members[other].id;
      boolean started = broker.connect(peerId, index, new ConnectListener() {
        @Override
        public void connected(MessageQueue queue) {
          link(peerId, queue);
        }

        @Override
        public void refused() {
          throw new IllegalStateException(id + " was refused by " + peerId);
        }
      });
      if (!started)
        throw new IllegalStateException(id + " could not reach " + peerId);
    }
  }

  void whenLinked(Runnable callback) {
    this.onLinked = callback;
    if (peers.size() == members.length - 1)
      callback.run();
  }

  void startSending() {
    task.post(new Runnable() {
      @Override
      public void run() {
        sendOne();
      }
    });
  }

  private void link(String peerId, MessageQueue queue) {
    peers.put(peerId, queue);
    queue.setListener(new MessageQueue.Listener() {
      @Override
      public void received(byte[] message) {
        onReceive(message);
      }

      @Override
      public void closed() {
      }
    });
    if (peers.size() == members.length - 1 && onLinked != null)
      onLinked.run();
  }

  private void sendOne() {
    if (sent >= messagesToSend)
      return;
    long timestamp = task.logicalClock();
    long messageId = (((long) index) << 32) | (sent + 1);
    byte[] payload = (id + "#" + sent).getBytes();
    MulticastMessage message = new MulticastMessage(messageId, id, 1L, timestamp, payload);
    observe(id, timestamp);
    pending.add(message);
    byte[] wire = MulticastMessage.encodeData(message);
    for (MessageQueue peer : peers.values())
      peer.send(wire, 0, wire.length, null);
    sent++;
    tryDeliver();
    if (sent >= messagesToSend)
      return;
    if (period <= 0)
      task.post(new Runnable() {
        @Override
        public void run() {
          sendOne();
        }
      });
    else
      task.post(new Runnable() {
        @Override
        public void run() {
          sendOne();
        }
      }, period);
  }

  private void onReceive(byte[] bytes) {
    Object incoming = MulticastMessage.decode(bytes);
    if (incoming instanceof MulticastMessage.Ack) {
      MulticastMessage.Ack ack = (MulticastMessage.Ack) incoming;
      observe(ack.senderId, ack.lamportTimestamp);
      tryDeliver();
      return;
    }
    MulticastMessage message = (MulticastMessage) incoming;
    if (message.generationId() != 1L)
      return;
    observe(message.senderId(), message.lamportTimestamp());
    if (deliveredIds.contains(message.messageId()))
      return;
    pending.add(message);
    acknowledge();
    tryDeliver();
  }

  private void acknowledge() {
    long timestamp = task.logicalClock();
    observe(id, timestamp);
    byte[] wire = MulticastMessage.encodeAck(id, timestamp);
    for (MessageQueue peer : peers.values())
      peer.send(wire, 0, wire.length, null);
  }

  private void observe(String senderId, long timestamp) {
    task.witness(timestamp);
    Long previous = lastSeen.get(senderId);
    if (previous == null || timestamp > previous)
      lastSeen.put(senderId, Long.valueOf(timestamp));
  }

  private void tryDeliver() {
    while (!pending.isEmpty()) {
      MulticastMessage head = pending.peek();
      if (!stable(head))
        return;
      pending.poll();
      if (!deliveredIds.add(Long.valueOf(head.messageId())))
        continue;
      verifier.delivered(id, head.messageId(), head.payload());
    }
  }

  private boolean stable(MulticastMessage message) {
    for (String member : group.members()) {
      Long seen = lastSeen.get(member);
      if (seen == null || seen.longValue() < message.lamportTimestamp())
        return false;
    }
    return true;
  }
}
