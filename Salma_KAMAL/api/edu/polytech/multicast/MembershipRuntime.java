package edu.polytech.multicast;

import java.util.ArrayList;
import java.util.List;

import edu.polytech.queues.Task;
import edu.polytech.queues.local.Boot;

/** Task creation and failure notifications. Membership decisions still use queues. */
final class MembershipRuntime {
  private final Verifier verifier;
  private final List<Actor> actors = new ArrayList<>();
  private Task detector;

  MembershipRuntime(Verifier verifier) {
    this.verifier = verifier;
  }

  synchronized void register(Actor actor) {
    actors.add(actor);
    Actor[] registered = actors.toArray(new Actor[0]);
    for (Actor member : actors)
      member.setRuntimeActors(registered);
  }

  synchronized Actor create(String id) {
    MembershipRequest.requireId(id);
    for (Actor actor : actors)
      if (actor.id().equals(id))
        throw new IllegalArgumentException("actor ID is already used");
    Actor actor = new Actor(id, actors.size(), new Boot().newTask(null, id),
        new Group(new ArrayList<String>()), verifier, actors.toArray(new Actor[0]), 0, 0);
    actor.setDetector(detector);
    actor.task().post(new Runnable() {
      @Override
      public void run() {
        actor.openBroker();
      }
    });
    return actor;
  }

  synchronized String brokerName(String actorId) {
    for (Actor actor : actors)
      if (actor.id().equals(actorId))
        return actor.brokerName();
    throw new IllegalArgumentException("unknown peer " + actorId);
  }

  void watch(final Actor actor, Task detectorTask) {
    detector = detectorTask;
    final Task watched = actor.task();
    detector.post(new Runnable() {
      @Override
      public void run() {
        final Actor[] original;
        synchronized (MembershipRuntime.this) {
          original = actors.toArray(new Actor[0]);
        }
        final Task.Listener listener = GroupManager.crashListener(actor, original);
        watched.set(new Task.Listener() {
          public void failed(Task task, Throwable failure) {
            if (actor.state() == Actor.State.LEFT && actor.leaveComplete())
              return;
            listener.failed(task, failure);
            synchronized (MembershipRuntime.this) {
              for (int i = original.length; i < actors.size(); i++) {
                final Actor peer = actors.get(i);
                if (!peer.task().dead())
                  peer.task().post(new Runnable() {
                    @Override
                    public void run() {
                      peer.onCrashNotice(actor.id());
                    }
                  });
              }
            }
          }

          public void completed(Task task, Object result) {
            listener.completed(task, result);
          }
        });
      }
    });
  }
}
