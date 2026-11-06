package edu.polytech.channels.local;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import edu.polytech.channels.Broker;
import edu.polytech.channels.Channel;
import edu.polytech.utils.CircularBuffer;

public class CBroker implements Broker {
  private static final BrokerManager manager = new BrokerManager();
  private final String name;
  private final Map<Integer, Queue<ConnectionRendezvous>> listeners = new HashMap<>();

  CBroker(String name) {
    this.name = name;
    manager.add(this);
  }

  
  private static class ConnectionRendezvous { // handling the meating point 
      CChannel serverChannel;
      CChannel clientChannel;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Channel connect(String name, int port) {
    CBroker targetBroker = manager.get(name);
    if (targetBroker == null)
      return null;
    synchronized (targetBroker) {
      // Put each connection in a queue so clients cannot overwrite one another
      // while the server is still accepting an earlier connection.
      Queue<ConnectionRendezvous> queue = targetBroker.listeners.get(port);
      if (queue == null) {
        queue = new LinkedList<>();
        targetBroker.listeners.put(port, queue);
      }
      ConnectionRendezvous rendezvous = new ConnectionRendezvous();
      CircularBuffer clientToServer = new CircularBuffer(1024);
      CircularBuffer serverToClient = new CircularBuffer(1024);
      CChannel.ConnectionState state = new CChannel.ConnectionState();
      // Each endpoint reads one buffer and writes to the other one.
      rendezvous.serverChannel = new CChannel(clientToServer, serverToClient, state);
      rendezvous.clientChannel = new CChannel(serverToClient, clientToServer, state);
      queue.add(rendezvous);
      targetBroker.notifyAll();
      return rendezvous.clientChannel;
    }
  }

  @Override
  public Channel accept(int port) {
    synchronized (this) {
      Queue<ConnectionRendezvous> queue = listeners.get(port);
      while (queue == null || queue.isEmpty()) {
        try {
          wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return null;
        }
        queue = listeners.get(port);
      }
      ConnectionRendezvous rendezvous = queue.remove();
      if (queue.isEmpty())
        listeners.remove(port);
      return rendezvous.serverChannel;
    }
  }

}
