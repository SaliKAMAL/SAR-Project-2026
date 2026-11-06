package edu.polytech.multicast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import edu.polytech.queues.Task;
import edu.polytech.queues.local.Boot;

/**
 * Builds the initial group, connects every pair, then is not used again.
 * No actor fails in the group this class builds.
 */
public final class GroupManager {

  public static final int MESSAGES_PER_ACTOR = 4;

  private static final AtomicInteger RUNS = new AtomicInteger();
  private static final long SETUP_TIMEOUT_MS = 5000L;

  private GroupManager() {
  }
  
  public static Started start(int n, int period) throws InterruptedException {
	  return start(n, period, MESSAGES_PER_ACTOR);
	}

  public static Started start(int n, int period, int messagesPerActor) throws InterruptedException {
    if (n < 1)
      throw new IllegalArgumentException("n");
    if (messagesPerActor < 1)
        throw new IllegalArgumentException("messagesPerActor");
    if (period < 0)
      throw new IllegalArgumentException("period");
    String prefix = "g" + RUNS.incrementAndGet() + "-";
    Boot boot = new Boot();
    Verifier verifier = new Verifier();
    List<String> ids = new ArrayList<String>();
    for (int i = 0; i < n; i++)
      ids.add(prefix + i);
    Group group = new Group(ids);
    Actor[] actors = new Actor[n];
    for (int i = 0; i < n; i++) {
      Task task = boot.newTask(null, ids.get(i));
      actors[i] = new Actor(ids.get(i), i, task, group, verifier, actors, period, messagesPerActor);
    }
 // PART 2 (Salma): The Perfect Failure Detector with Synchronous Registration
    final Actor[] finalActors = actors;
    Task detectorTask = boot.newTask(null, "detector");
    for (int i = 0; i < actors.length; i++)
        actors[i].setDetector(detectorTask); // Part 3: needed to watch the task of a respawned actor
    final java.util.concurrent.CountDownLatch detectorReady = new java.util.concurrent.CountDownLatch(1);
    
    

    detectorTask.post(new Runnable() {
        @Override
        public void run() {
          for (int i = 0; i < finalActors.length; i++) {
            final Actor actor = finalActors[i];
            actor.task().set(crashListener(actor, finalActors));
          }
          detectorReady.countDown(); // Signale que tous les listeners sont branchés
        }
      });
    try {
      detectorReady.await(); // On fige le démarrage du monde tant que le détecteur n'est pas prêt !
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    await(postEach(actors, new Job() {
      @Override
      public void run(Actor actor) {
        actor.openBroker();
      }
    }));
    await(postEach(actors, new Job() {
      @Override
      public void run(Actor actor) {
        actor.bindToHigherPeers();
        actor.bindRejoinPorts();
      }
    }));
    final CountDownLatch linked = new CountDownLatch(n);
    for (int i = 0; i < n; i++) {
      actors[i].whenLinked(new Runnable() {
        @Override
        public void run() {
          linked.countDown();
        }
      });
    }
    await(postEach(actors, new Job() {
      @Override
      public void run(Actor actor) {
        actor.connectToLowerPeers();
      }
    }));
    if (n > 1 && !linked.await(SETUP_TIMEOUT_MS, TimeUnit.MILLISECONDS))
      throw new IllegalStateException("group links were not established");
    await(postEach(actors, new Job() {
      @Override
      public void run(Actor actor) {
        actor.startSending();
      }
    }));
    List<Actor> members = new ArrayList<Actor>();
    Collections.addAll(members, actors);
    return new Started(group, verifier, Collections.unmodifiableList(members));
  }
  
  /**
   * The perfect failure detector for one actor. It runs on the detector task when the task of that
   * actor dies. Part 3: it also marks the actor DEAD, and it is set again on the task of a respawned actor.
   */
  static Task.Listener crashListener(final Actor actor, final Actor[] all) {
    return new Task.Listener() {
      @Override
      public void failed(Task t, Throwable th) {
        actor.markDead();
        System.out.println(">>> DETECTOR: " + actor.id() + " has crashed! Notifying peers...");
        for (int j = 0; j < all.length; j++) {
          final Actor peer = all[j];
          if (!peer.id().equals(actor.id()) && !peer.task().dead()) {
            peer.task().post(new Runnable() {
              @Override
              public void run() {
                peer.onCrashNotice(actor.id());
              }
            });
          }
        }
      }

      @Override
      public void completed(Task t, Object o) {
      }
    };
  }

  private static CountDownLatch postEach(Actor[] actors, final Job job) {
    final CountDownLatch done = new CountDownLatch(actors.length);
    for (int i = 0; i < actors.length; i++) {
      final Actor actor = actors[i];
      actor.task().post(new Runnable() {
        @Override
        public void run() {
          job.run(actor);
          done.countDown();
        }
      });
    }
    return done;
  }

  private static void await(CountDownLatch latch) throws InterruptedException {
    if (!latch.await(SETUP_TIMEOUT_MS, TimeUnit.MILLISECONDS))
      throw new IllegalStateException("group setup timed out");
  }

  private interface Job {
    void run(Actor actor);
  }

  public static final class Started {
    private final Group group;
    private final Verifier verifier;
    private final List<Actor> actors;

    Started(Group group, Verifier verifier, List<Actor> actors) {
      this.group = group;
      this.verifier = verifier;
      this.actors = actors;
    }

    public Group group() {
      return group;
    }

    public Verifier verifier() {
      return verifier;
    }

    public List<Actor> actors() {
      return actors;
    }
  }
}
