package edu.polytech.multicast;

import java.util.HashMap;

import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.List;

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
  private volatile Task task;
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
  //Part 2 state for flush mechanism
  private final Set<String> flushingDeadActors = new HashSet<String>();
  private final Map<String, Map<String, Set<Long>>> flushSets = new HashMap<String, Map<String, Set<Long>>>();
  
  public enum State { IN_VIEW,DEAD,RESPAWNED}
  private volatile State state = State.IN_VIEW;
  private volatile long generationId=1L;

  Actor(String id, int index, Task task, Group group, Verifier verifier, Actor[] members, int period,
      int messagesToSend) {
    this.id = id;
    this.index = index;
    this.task = task;
    this.group = new Group(group.members());
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
  
  public State state() {
	  return state;
  }
  
  public long generationId() {
	  return generationId;
  }

  void openBroker() {
    broker = task.newBroker(id);
  }
  
  public void respawn() {
	  throw new UnsupportedOperationException("Chaimae : respawn() is not implemented yet");
  }
  
  public void reinsert() {
	  throw new UnsupportedOperationException("Chaimae : reinsert() is not implemented yet");
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
  
  /**
   * Triggered by the failure detector when a crash is noticed.
   * A survivor does not delete the member on the first notice.
   * It starts the flush phase by broadcasting its pending messages from the crashed sender.
   */
  void onCrashNotice(String crashedId) {
    try {
      if (!group.members().contains(crashedId) || flushingDeadActors.contains(crashedId)) {
        return;
      }
	      
      flushingDeadActors.add(crashedId);
      flushSets.put(crashedId, new java.util.HashMap<String, java.util.Set<Long>>());

      long timestamp = task.logicalClock();
      observe(id, timestamp);
	      
      int count = 0;
      for (MulticastMessage msg : pending) {
        if (msg.senderId().equals(crashedId)) count++;
      }
	      
      long[] pendingIds = new long[count];
      int index = 0;
      for (MulticastMessage msg : pending) {
        if (msg.senderId().equals(crashedId)) {
          pendingIds[index++] = msg.messageId();
        }
      }

      byte[] wire = MulticastMessage.encodeFlush(id, crashedId, timestamp, pendingIds);
      for (MessageQueue peer : peers.values()) {
        peer.send(wire, 0, wire.length, null);
      }
	      
      processFlushSet(id, crashedId, pendingIds);
	      
    } catch (Exception e) {
      System.err.println(">>> FATAL ERROR in onCrashNotice for actor " + this.id + ": " + e);
      e.printStackTrace();
    }
  }

  /**
   * Part 2: Collect flush sets from survivors.
   */
  private void processFlushSet(String senderId, String crashedId, long[] pendingIds) {
    if (!group.members().contains(crashedId)) {
      return; // Already resolved
    }

    Map<String, Set<Long>> setsForDead = flushSets.get(crashedId);
    if (setsForDead == null) {
      // If we receive a flush before our local detector fired, trigger it now
      onCrashNotice(crashedId);
      setsForDead = flushSets.get(crashedId);
	      
      // Ajout Salma (Part 2) : Protection si la résolution a été synchrone !
      if (setsForDead == null) {
          return;
      }
    }

    Set<Long> set = new HashSet<Long>();
    for (long msgId : pendingIds) {
      set.add(msgId);
    }
    setsForDead.put(senderId, set);

    // Check if we have received the flush set from ALL alive members (except the dead one)
    List<String> currentMembers = group.members();
    Set<String> expectedSurvivors = new HashSet<String>(currentMembers);
    expectedSurvivors.remove(crashedId);

    if (setsForDead.keySet().containsAll(expectedSurvivors)) {
      executeIntersectionRule(crashedId, setsForDead, expectedSurvivors);
    }
  }
  
  /**
   * Part 2: Absolute intersection and view shrinking.
   */
  private void executeIntersectionRule(String crashedId, Map<String, Set<Long>> setsForDead, Set<String> survivors) {
    // 1. Calculate the absolute intersection of pending message IDs
    Set<Long> intersection = null;
    for (String survivor : survivors) {
      Set<Long> survivorSet = setsForDead.get(survivor);
      if (intersection == null) {
        intersection = new HashSet<Long>(survivorSet);
      } else {
        intersection.retainAll(survivorSet);
      }
    }

    // 2. Rebuild the pending queue, keeping only the dead actor's messages that are in the intersection
    PriorityQueue<MulticastMessage> rebuiltQueue = new PriorityQueue<MulticastMessage>();
    while (!pending.isEmpty()) {
      MulticastMessage msg = pending.poll();
      if (msg.senderId().equals(crashedId)) {
        if (intersection != null && intersection.contains(msg.messageId())) {
          rebuiltQueue.add(msg);
        }
      } else {
        rebuiltQueue.add(msg);
      }
    }
    pending.addAll(rebuiltQueue);

    // 3. Clear flush state
    flushingDeadActors.remove(crashedId);
    flushSets.remove(crashedId);

    // 4. Shrink the group and notify the Verifier
    group.remove(crashedId);
    verifier.crashed(crashedId);

    // 5. With the dead actor gone from the view, the stability rule will now pass. Try to deliver!
    tryDeliver();
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
	    try {
	      Object incoming = MulticastMessage.decode(bytes);
	      
	      if (incoming instanceof MulticastMessage.Ack) {
	        MulticastMessage.Ack ack = (MulticastMessage.Ack) incoming;
	        observe(ack.senderId, ack.lamportTimestamp);
	        tryDeliver();
	        return;
	      }
	      
	      // Part 2: Intercept Flush messages
	      if (incoming instanceof MulticastMessage.Flush) {
	        MulticastMessage.Flush flush = (MulticastMessage.Flush) incoming;
	        observe(flush.senderId, flush.lamportTimestamp);
	        processFlushSet(flush.senderId, flush.crashedId, flush.pendingMessageIds);
	        return; // CRUCIAL : Ne pas oublier ce return, sinon ClassCastException juste en dessous !
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
	      
	    } catch (Exception e) {
	      System.err.println(">>> FATAL ERROR in onReceive for actor " + this.id + ": " + e);
	      e.printStackTrace();
	    }
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
