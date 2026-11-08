package edu.polytech.multicast;

import java.util.HashMap;
import edu.polytech.queues.local.Boot;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

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
  
  /** A respawned actor connects to port REJOIN_PORT_BASE + its index on the broker of each member. */
  static final int REJOIN_PORT_BASE = 1000;
  /** Queues opened by a respawned actor. They join 'peers' only when the actor is in the view. */
  private final Map<String, MessageQueue> rejoinQueues = new HashMap<String, MessageQueue>();
  private volatile String brokerName;
  private Task detectorTask;
  private boolean respawning;
  private boolean respawnFailable;
  private int linksToWait;
  
  /** What to do when every queue asked by connectToMember is open. */
  private Runnable afterLinks;
  /** Generation of the member each queue of 'peers' leads to (used by a respawned actor). */
  private final Map<String, Long> peerGeneration = new HashMap<String, Long>();
  /** True from the start of our reinsertion until we are in the view. Only one actor holds it. */
  private volatile boolean holdsTurn;
  /** Crash flush messages about an actor whose reinsertion we have not installed yet. */
  private final List<Object[]> deferredFlushes = new ArrayList<Object[]>();
  
//---- Part 3: reinsertion = a view change, at most one at a time ----
 /** What a view change is waiting for: the answer (ViewFlush) of every member and of the subject. */
 private static final class ViewChange {
   final String subjectId;
   final long subjectGeneration;
   final Set<String> expected = new HashSet<String>();
   final Map<String, MulticastMessage.ViewFlush> replies = new HashMap<String, MulticastMessage.ViewFlush>();

   ViewChange(String subjectId, long subjectGeneration) {
     this.subjectId = subjectId;
     this.subjectGeneration = subjectGeneration;
   }
 }

 private ViewChange viewChange;
 /** True from our answer to a view change until the new view is installed: we do not multicast. */
 private boolean frozen;
 private volatile boolean reinserting;
 private final List<Object> deferredViewMessages = new ArrayList<Object>();
 private final List<byte[]> deferredSends = new ArrayList<byte[]>();
 private final List<GroupListener> listeners = new CopyOnWriteArrayList<GroupListener>();
  
  /**
   * Generation accepted for each member of the view. A message or an acknowledgement is ignored
   * if its sender is not in this map or if its generation is not the accepted one.
   */
  private final Map<String, Long> acceptedGeneration = new HashMap<String, Long>();
  
  private static final long MANUAL_ID_BIT = 1L << 31;
  private int manualSent;

  Actor(String id, int index, Task task, Group group, Verifier verifier, Actor[] members, int period,
      int messagesToSend) {
    this.id = id;
    this.index = index;
    this.task = task;
    this.group = new Group(group.members());
    this.brokerName = id;
    for (String member : group.members())
        acceptedGeneration.put(member, Long.valueOf(1L));
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
  
  static long messageId(long generation, int index, long sequence) {
	    return (generation << 48) | (((long) index) << 32) | sequence;
	  }
 
  
  /** Part 3: listeners are told about resurrected and reinserted. */
  public void addListener(GroupListener listener) {
    listeners.add(listener);
  }
  
  /** For tests, to be called on the task of this actor: is this payload waiting, not delivered yet? */
  boolean holdsPayload(String payload) {
    for (MulticastMessage message : pending)
      if (new String(message.payload()).equals(payload))
        return true;
    return false;
  }
  
  /** True if the sender is in the view and the generation is the one we accept for it. */
  private boolean accepts(String senderId, long generation) {
	  if (state == State.RESPAWNED) return peers.containsKey(senderId); // before its view is installed, it listens to the members
      if (viewChange != null && senderId.equals(viewChange.subjectId) && generation == viewChange.subjectGeneration)
        return true; // the actor that comes back may be faster than us to install the view
      Long accepted = acceptedGeneration.get(senderId);
      return accepted != null && accepted.longValue() == generation;
  }

  /** Sends to the members of the view only. The others (dead, outside the view) get nothing. */
  private void broadcast(byte[] wire) {
    for (Map.Entry<String, MessageQueue> peer : peers.entrySet()) {
      if (group.members().contains(peer.getKey()))
        peer.getValue().send(wire, 0, wire.length, null);
    }
  }
  
  public void multicast(final byte[] payload) {
	    final byte[] copy = payload.clone();
	    task.post(new Runnable() {
	      @Override
	      public void run() {
	        sendManual(copy);
	      }
	    });
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
  
  /**
   * Part 3, step 1: resurrection. Allowed only when state() == DEAD, otherwise IllegalStateException.
   * Creates a new task (watched by the failure detector), a new broker and new queues to the members,
   * empties the message queue and the acknowledgement table, increments generationId, calls
   * Verifier.resurrected(id). The actor stays outside the view (RESPAWNED).
   * It returns at once: wait for state() == RESPAWNED.
   */
  public void respawn() {
    synchronized (this) {
      if (state != State.DEAD || respawning)
        throw new IllegalStateException(id + " is " + state + ": respawn() needs a dead actor");
      if (detectorTask == null)
        throw new IllegalStateException(id + " has no failure detector");
      respawning = true;
    }
    final long newGeneration = generationId + 1;
    final boolean failable = task.mayFail();
    final Task fresh = new Boot().newTask(null, id + "@" + newGeneration);
    final Actor self = this;
    detectorTask.post(new Runnable() {
      @Override
      public void run() {
        fresh.set(GroupManager.crashListener(self, members)); // the detector watches the new task
      }
    });
    fresh.post(new Runnable() {
      @Override
      public void run() {
        startNewLife(fresh, newGeneration, failable);
      }
    });
  }

  /**
   * Part 3, step 2: reinsertion. Allowed only when state() == RESPAWNED, otherwise IllegalStateException.
   * View change ordered with the messages by a flush. Then the actor is IN_VIEW and
   * delivers only messages whose timestamp is greater than the view timestamp.
   */
  public void reinsert() {
    synchronized (this) {
      if (state != State.RESPAWNED || reinserting)
        throw new IllegalStateException(id + " is " + state + ": reinsert() needs a respawned actor");
      reinserting = true;
    }
    task.post(new Runnable() {
      @Override
      public void run() {
    	  startReinsertion();
      }
    });
  }
  
  private void startReinsertion() {
	    if (state != State.RESPAWNED)
	      return;
	    if (!takeTurn()) { // another actor is being reinserted: one at a time
	      task.post(new Runnable() {
	        @Override
	        public void run() {
	          startReinsertion();
	        }
	      }, 5);
	      return;
	    }
	    refreshConnections();
	  }

	  private boolean takeTurn() {
	    synchronized (members) {
	      for (Actor other : members)
	        if (other != this && other.holdsTurn)
	          return false;
	      holdsTurn = true;
	      return true;
	    }
	  }

	  private void refreshConnections() {
	    for (Iterator<Map.Entry<String, MessageQueue>> it = peers.entrySet().iterator(); it.hasNext();) {
	      String peerId = it.next().getKey();
	      Actor other = actorById(peerId);
	      Long known = peerGeneration.get(peerId);
	      if (other == null || other.state() != State.IN_VIEW || known == null
	          || known.longValue() != other.generationId()) {
	        it.remove();
	        peerGeneration.remove(peerId);
	      }
	    }
	    List<Actor> missing = new ArrayList<Actor>();
	    for (Actor other : members)
	      if (other != this && other.state() == State.IN_VIEW && !peers.containsKey(other.id))
	        missing.add(other);
	    linksToWait = missing.size();
	    afterLinks = new Runnable() {
	      @Override
	      public void run() {
	        beginOwnReinsertion();
	      }
	    };
	    if (linksToWait == 0) {
	      beginOwnReinsertion();
	      return;
	    }
	    for (Actor other : missing)
	      connectToMember(other);
	  }

	  private Actor actorById(String actorId) {
	    for (Actor other : members)
	      if (other.id.equals(actorId))
	        return other;
	    return null;
	  }

	  private boolean memberGone(String memberId) {
	    Actor other = actorById(memberId);
	    if (other == null || other.task().dead())
	      return true;
	    Long known = peerGeneration.get(memberId);
	    return known != null && known.longValue() != other.generationId();
	  }

	  private boolean subjectGone(String subjectId, long subjectGeneration) {
	    Actor other = actorById(subjectId);
	    return other != null && (other.generationId() != subjectGeneration || other.task().dead());
	  }

  /**
   * Runs on the task of the respawned actor. It asks every member to change the view and gives its
   * own answer to the flush. It is installed in the view when it has the answer of every member.
   */
  private void beginOwnReinsertion() {
	    if (state != State.RESPAWNED)
	        return;
	      for (Iterator<Map.Entry<String, MessageQueue>> it = peers.entrySet().iterator(); it.hasNext();) {
	        String peerId = it.next().getKey();
	        if (memberGone(peerId)) {
	          it.remove();
	          peerGeneration.remove(peerId);
	        }
	      }
    ViewChange change = new ViewChange(id, generationId);
    change.expected.addAll(peers.keySet());
    change.expected.add(id);
    viewChange = change;
    long timestamp = task.logicalClock();
    byte[] request = MulticastMessage.encodeReinsertRequest(id, generationId, timestamp);
    byte[] flush = MulticastMessage.encodeViewFlush(id, generationId, id, generationId, timestamp);
    for (MessageQueue peer : peers.values()) {
      peer.send(request, 0, request.length, null);
      peer.send(flush, 0, flush.length, null);
    }
    change.replies.put(id, new MulticastMessage.ViewFlush(id, generationId, id, generationId, timestamp));
    completeOwnReinsertionIfReady();
  }

  /** The respawned actor is back in the view. It never delivers a message with a timestamp <= V. */
  private void completeOwnReinsertionIfReady() {
    ViewChange change = viewChange;
    if (change == null || !change.replies.keySet().containsAll(change.expected))
      return;
    long view = viewTimestamp(change);
    task.witness(view);
    for (Iterator<MulticastMessage> it = pending.iterator(); it.hasNext();)
      if (it.next().lamportTimestamp() <= view)
        it.remove();
    group.reset(change.expected);
    acceptedGeneration.clear();
    for (MulticastMessage.ViewFlush reply : change.replies.values()) {
        if (!change.expected.contains(reply.senderId))
            continue; // a member that died: it is not in our view
        acceptedGeneration.put(reply.senderId, Long.valueOf(reply.senderGeneration));
        raiseLastSeen(reply.senderId, view);
    }
    viewChange = null;
    synchronized (this) {
      reinserting = false;
      state = State.IN_VIEW;
    }
    holdsTurn = false;
    for (GroupListener listener : listeners)
      listener.reinserted(id);
    verifier.reinserted(id);
    // from the next runnable on, our timestamps are greater than the view timestamp
    task.post(new Runnable() {
      @Override
      public void run() {
        if (state != State.IN_VIEW)
          return;
        acknowledge(); // tells the members we are there, so nobody waits for us
        tryDeliver();
      }
    });
  }

  /** A member receives a reinsertion request or an answer to a view change. */
  private void handleViewMessage(Object message) {
    long timestamp = message instanceof MulticastMessage.ReinsertRequest
        ? ((MulticastMessage.ReinsertRequest) message).lamportTimestamp
        : ((MulticastMessage.ViewFlush) message).lamportTimestamp;
    task.witness(timestamp);
    if (state == State.RESPAWNED) { // we only wait for the answers to our own request
      if (message instanceof MulticastMessage.ViewFlush && viewChange != null) {
        MulticastMessage.ViewFlush reply = (MulticastMessage.ViewFlush) message;
        Long known = peerGeneration.get(reply.senderId);
        if (known != null && known.longValue() == reply.senderGeneration)
          viewChange.replies.put(reply.senderId, reply);
        completeOwnReinsertionIfReady();
      }
      return;
    }
    if (state != State.IN_VIEW)
      return;
    String subject;
    long subjectGeneration;
    if (message instanceof MulticastMessage.ReinsertRequest) {
      subject = ((MulticastMessage.ReinsertRequest) message).requesterId;
      subjectGeneration = ((MulticastMessage.ReinsertRequest) message).generationId;
    } else {
      subject = ((MulticastMessage.ViewFlush) message).subjectId;
      subjectGeneration = ((MulticastMessage.ViewFlush) message).subjectGeneration;
    }
    if (viewChange == null) {
      if (group.members().contains(subject)) {
        deferredViewMessages.add(message); // we have not finished the crash flush of this actor yet
        return;
      }
      beginViewChange(subject, subjectGeneration);
    } else if (!viewChange.subjectId.equals(subject) || viewChange.subjectGeneration != subjectGeneration) {
      deferredViewMessages.add(message); // one view change at a time
      return;
    }
    if (message instanceof MulticastMessage.ViewFlush) {
      MulticastMessage.ViewFlush reply = (MulticastMessage.ViewFlush) message;
      viewChange.replies.put(reply.senderId, reply);
    }
    completeViewChangeIfReady();
  }

  /** We stop multicasting and answer the flush to every member and to the actor that comes back. */
  private void beginViewChange(String subject, long subjectGeneration) {
    ViewChange change = new ViewChange(subject, subjectGeneration);
    change.expected.addAll(group.members());
    change.expected.add(subject);
    if (subjectGone(subject, subjectGeneration))
        change.expected.remove(subject);
    viewChange = change;
    frozen = true;
    long timestamp = task.logicalClock();
    observe(id, timestamp);
    byte[] flush = MulticastMessage.encodeViewFlush(id, generationId, subject, subjectGeneration, timestamp);
    broadcast(flush);
    MessageQueue back = rejoinQueues.get(subject);
    if (back != null)
      back.send(flush, 0, flush.length, null);
    change.replies.put(id, new MulticastMessage.ViewFlush(id, generationId, subject, subjectGeneration, timestamp));
  }

  /**
   * With the answer of every member we hold every message sent before the view change (barrier).
   * Those have a timestamp <= V: we deliver them, then install the view. Every message sent from now on
   * has a timestamp > V.
   */
  private void completeViewChangeIfReady() {
    ViewChange change = viewChange;
    if (change == null || !change.replies.keySet().containsAll(change.expected))
      return;
    long view = viewTimestamp(change);
    task.witness(view);
    deliverUpTo(view);
    group.add(change.subjectId);
    acceptedGeneration.put(change.subjectId, Long.valueOf(change.subjectGeneration));
    raiseLastSeen(change.subjectId, view);
    MessageQueue back = rejoinQueues.remove(change.subjectId);
    if (back != null)
      peers.put(change.subjectId, back);
    viewChange = null;
    frozen = false;
    if (subjectGone(change.subjectId, change.subjectGeneration))
        onCrashNotice(change.subjectId);
      replayDeferredFlushes();
    tryDeliver();
    // from the next runnable on, our timestamps are greater than the view timestamp
    task.post(new Runnable() {
      @Override
      public void run() {
        resumeAfterViewChange();
      }
    });
  }
  private boolean waitingToInstall(String actorId) {
	    if (viewChange != null && viewChange.subjectId.equals(actorId))
	      return true;
	    for (Object message : deferredViewMessages) {
	      String subject = message instanceof MulticastMessage.ReinsertRequest
	          ? ((MulticastMessage.ReinsertRequest) message).requesterId
	          : ((MulticastMessage.ViewFlush) message).subjectId;
	      if (subject.equals(actorId))
	        return true;
	    }
	    return false;
	  }

	  private void replayDeferredFlushes() {
	    if (deferredFlushes.isEmpty())
	      return;
	    List<Object[]> flushes = new ArrayList<Object[]>(deferredFlushes);
	    deferredFlushes.clear();
	    for (Object[] flush : flushes)
	      processFlushSet((String) flush[0], (String) flush[1], (long[]) flush[2]);
	  }

  private void resumeAfterViewChange() {
    if (state != State.IN_VIEW || frozen)
      return;
    // Tell everybody, the actor that came back included, how far we have seen. Without this, an ack
    // sent before the view was installed never reached the new member, and it could wait forever.
    acknowledge();
    tryDeliver();
    // One runnable per message: the clock ticks for each of them, so their timestamps differ.
    List<byte[]> sends = new ArrayList<byte[]>(deferredSends);
    deferredSends.clear();
    for (final byte[] payload : sends) {
      task.post(new Runnable() {
        @Override
        public void run() {
          sendManual(payload);
        }
      });
    }
    redispatchDeferredViewMessages();
  }

  private void redispatchDeferredViewMessages() {
    if (viewChange != null || deferredViewMessages.isEmpty())
      return;
    List<Object> messages = new ArrayList<Object>(deferredViewMessages);
    deferredViewMessages.clear();
    for (Object message : messages)
      handleViewMessage(message);
  }

  private static long viewTimestamp(ViewChange change) {
    long view = 0;
    for (MulticastMessage.ViewFlush reply : change.replies.values())
      view = Math.max(view, reply.lamportTimestamp);
    return view;
  }

  private void raiseLastSeen(String memberId, long timestamp) {
    Long previous = lastSeen.get(memberId);
    if (previous == null || previous.longValue() < timestamp)
      lastSeen.put(memberId, Long.valueOf(timestamp));
  }

  private void deliverUpTo(long limit) {
    while (!pending.isEmpty() && pending.peek().lamportTimestamp() <= limit) {
      MulticastMessage head = pending.poll();
      if (!deliveredIds.add(Long.valueOf(head.messageId())))
        continue;
      verifier.delivered(id, head.messageId(), head.payload());
    }
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
  
  void setDetector(Task detectorTask) {
	    this.detectorTask = detectorTask;
	  }

	  /** Called by the failure detector when the task of this actor is dead. */
	  void markDead() {
		  holdsTurn = false;
	      state = State.DEAD;
	  }

	  String brokerName() {
	    return brokerName;
	  }

	  /** Every actor accepts, for each other actor, the connection of a respawned actor. */
	  void bindRejoinPorts() {
	    for (int other = 0; other < members.length; other++) {
	      if (other == index)
	        continue;
	      final String peerId = members[other].id;
	      boolean bound = broker.bind(REJOIN_PORT_BASE + other, new BindListener() {
	        @Override
	        public void accepted(MessageQueue queue) {
	          rejoinQueues.put(peerId, queue);
	          queue.setListener(listener());
	        }

	        @Override
	        public void unbound() {
	        }
	      });
	      if (!bound)
	        throw new IllegalStateException(id + " could not bind rejoin port for " + peerId);
	    }
	  }

	  /** Runs on the new task. The actor comes back empty and outside the view. */
	  private void startNewLife(Task fresh, long newGeneration, boolean failable) {
	    task = fresh;
	    generationId = newGeneration;
	    brokerName = id + "@" + newGeneration;
	    respawnFailable = failable;
	    pending.clear();
	    lastSeen.clear();
	    deliveredIds.clear();
	    acceptedGeneration.clear(); // nobody is accepted until the actor is in a view
	    flushingDeadActors.clear();
	    flushSets.clear();
	    peers.clear();
	    rejoinQueues.clear();
	    peerGeneration.clear();
	    deferredFlushes.clear();
	    holdsTurn = false;
	    viewChange = null;                
	    frozen = false;                   
	    reinserting = false;              
	    deferredViewMessages.clear();     
	    deferredSends.clear();
	    sent = messagesToSend; // the new life does not replay the periodic load
	    manualSent = 0;
	    broker = task.newBroker(brokerName); // a broker name can not be used twice
	    bindRejoinPorts();
	    linksToWait = 0;
	    afterLinks = new Runnable() {
	      @Override
	      public void run() {
	        finishRespawn();
	      }
	    };
	    List<Actor> inView = new ArrayList<Actor>();
	    for (Actor other : members)
	      if (other != this && other.state() == State.IN_VIEW)
	        inView.add(other);
	    linksToWait = inView.size();
	    if (linksToWait == 0)
	      finishRespawn();
	    for (Actor other : inView)
	      connectToMember(other);
	  }

	  private void connectToMember(final Actor other) {
	    final long generation = other.generationId();
	    boolean started = broker.connect(other.brokerName(), REJOIN_PORT_BASE + index, new ConnectListener() {
	      @Override
	      public void connected(MessageQueue queue) {
	        peers.put(other.id, queue);
	        peerGeneration.put(other.id, Long.valueOf(generation));
	        queue.setListener(listener());
	        oneLinkDone();
	      }

	      @Override
	      public void refused() {
	        oneLinkDone();
	      }
	    });
	    if (!started)
	      oneLinkDone();
	  }

	  private void oneLinkDone() {
	    if (--linksToWait == 0)
	      afterLinks.run();
	  }

	  private void finishRespawn() {
	    task.setMayFail(respawnFailable); // failable again only now, so setup can not be killed at random
	    verifier.resurrected(id);
	    for (GroupListener listener : listeners)  
	        listener.resurrected(id);
	    synchronized (this) {
	      respawning = false;
	      state = State.RESPAWNED;
	    }
	  }
	  
	  private MessageQueue.Listener listener() {
		    return new MessageQueue.Listener() {
		      @Override
		      public void received(byte[] message) {
		        onReceive(message);
		      }

		      @Override
		      public void closed() {
		      }
		    };
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
	    if (state == State.RESPAWNED) {
	        ViewChange change = viewChange;
	        if (change != null && !crashedId.equals(id) && change.expected.contains(crashedId) && memberGone(crashedId)) {
	          change.expected.remove(crashedId);
	          change.replies.remove(crashedId);
	          completeOwnReinsertionIfReady();
	        }
	        return;
	      }
	      if (state != State.IN_VIEW)
	        return;
	      ViewChange pendingChange = viewChange;
	      if (pendingChange != null && pendingChange.subjectId.equals(crashedId) && !group.members().contains(crashedId)) {
	        if (subjectGone(pendingChange.subjectId, pendingChange.subjectGeneration)) {
	          pendingChange.expected.remove(crashedId);
	          completeViewChangeIfReady();
	        }
	        return;
	      }
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
      broadcast(wire);
	      
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
	        if (waitingToInstall(crashedId))
	          deferredFlushes.add(new Object[] { senderId, crashedId, pendingIds });
	        return; // Already resolved
	      }

    java.util.Map<String, java.util.Set<Long>> setsForDead = flushSets.get(crashedId);
    if (setsForDead == null) {
      // If we receive a flush before our local detector fired, trigger it now
      onCrashNotice(crashedId);
      setsForDead = flushSets.get(crashedId);
        
      // Added by Salma (Part 2): Ensure protection when the resolution is synchronous
      if (setsForDead == null) {
          return;
      }
    }

    java.util.Set<Long> set = new java.util.HashSet<Long>();
    for (long msgId : pendingIds) {
      set.add(msgId);
    }
    setsForDead.put(senderId, set);

    // Check all pending flush operations whenever a message is received.
    tryResolveFlushes();
  }
  
/**
 * Tries to resolve all pending flush operations.
 * When multiple actors crash, they are no longer expected to wait for one another.
 */
  private void tryResolveFlushes() {
    boolean progress;
    do {
      progress = false;
      // Copy the collection to avoid ConcurrentModificationException during resolution.
      java.util.List<String> deadActors = new java.util.ArrayList<String>(flushingDeadActors);
      
      for (String deadId : deadActors) {
        if (!group.members().contains(deadId)) {
          continue; // Already resolved
        }
        
        java.util.Map<String, java.util.Set<Long>> setsForDead = flushSets.get(deadId);
        if (setsForDead == null) continue;

        // The key idea: wait for the lists from all active members, excluding the ones that crashed.
        java.util.Set<String> expectedSurvivors = new java.util.HashSet<String>(group.members());
        expectedSurvivors.removeAll(flushingDeadActors);

        if (setsForDead.keySet().containsAll(expectedSurvivors)) {
          executeIntersectionRule(deadId, setsForDead, expectedSurvivors);
          progress = true;
          break; 
        }
      }
    } while (progress);
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
    acceptedGeneration.remove(crashedId);
    lastSeen.remove(crashedId);
    verifier.crashed(crashedId);
    if (viewChange != null) { // a member died during a view change: we do not wait for its answer 
        viewChange.expected.remove(crashedId);                                                         
        completeViewChangeIfReady();                                                                   
      }

    // 5. With the dead actor gone from the view, the stability rule will now pass. Try to deliver!
    tryDeliver();
    redispatchDeferredViewMessages(); // a reinsertion request may have been waiting for this crash flush
  }
  
  private void link(String peerId, MessageQueue queue) {
    peers.put(peerId, queue);
    queue.setListener(listener());
    if (peers.size() == members.length - 1 && onLinked != null)
      onLinked.run();
  }

  private void sendOne() {
    if (sent >= messagesToSend)
      return;
    if (frozen) { // a view change is running: try again soon
        task.post(new Runnable() {                             
          @Override                                            
          public void run() {                                  
            sendOne();                                         
          }                                                    
        }, 5);                                                 
        return;                                                
      }
    long timestamp = task.logicalClock();
    long messageId = messageId(generationId, index, sent + 1);
    byte[] payload = (id + "#" + sent).getBytes();
    MulticastMessage message = new MulticastMessage(messageId, id, generationId, timestamp, payload);
    observe(id, timestamp);
    pending.add(message);
    byte[] wire = MulticastMessage.encodeData(message);
    broadcast(wire);
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
  
  private void sendManual(byte[] payload) {
	  if (state != State.IN_VIEW)
		  return; // a respawned actor does not multicast before it is reinserted
	  if (frozen) {                  
		    deferredSends.add(payload);  
		    return;                      
		  }
	    long timestamp = task.logicalClock();
	    long messageId = messageId(generationId, index, MANUAL_ID_BIT | (++manualSent));
	    MulticastMessage message = new MulticastMessage(messageId, id, generationId, timestamp, payload);
	    observe(id, timestamp);
	    pending.add(message);
	    byte[] wire = MulticastMessage.encodeData(message);
	    broadcast(wire);
	    tryDeliver();
	  }

   void onReceive(byte[] bytes) {
	    try {
	      Object incoming = MulticastMessage.decode(bytes);
	      
	      if (incoming instanceof MulticastMessage.Ack) {
	        MulticastMessage.Ack ack = (MulticastMessage.Ack) incoming;
	        if (!accepts(ack.senderId, ack.generationId))
	            return;
	        observe(ack.senderId, ack.lamportTimestamp);
	        tryDeliver();
	        return;
	      }
	      
	   // Part 3: reinsertion                                                          
          if (incoming instanceof MulticastMessage.ReinsertRequest || incoming instanceof MulticastMessage.ViewFlush) {
            handleViewMessage(incoming);                                                  
            return;                                                                       
          }
	      
	      // Part 2: Intercept Flush messages
	      if (incoming instanceof MulticastMessage.Flush) {
	        MulticastMessage.Flush flush = (MulticastMessage.Flush) incoming;
	        observe(flush.senderId, flush.lamportTimestamp);
	        processFlushSet(flush.senderId, flush.crashedId, flush.pendingMessageIds);
	        return; 
	      }
	      
	      MulticastMessage message = (MulticastMessage) incoming;
	      if (!accepts(message.senderId(), message.generationId()))
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
	  if (state != State.IN_VIEW)
		    return;
    long timestamp = task.logicalClock();
    observe(id, timestamp);
    byte[] wire = MulticastMessage.encodeAck(id, generationId, timestamp);
    broadcast(wire);
  }

  private void observe(String senderId, long timestamp) {
    task.witness(timestamp);
    Long previous = lastSeen.get(senderId);
    if (previous == null || timestamp > previous)
      lastSeen.put(senderId, Long.valueOf(timestamp));
  }

  private void tryDeliver() {
	  if (state != State.IN_VIEW || viewChange != null)
		    return;
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
