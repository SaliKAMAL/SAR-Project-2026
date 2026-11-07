package edu.polytech.channels.local;

import java.util.HashMap;
import java.util.Map;

public class BrokerManager {

  private static final Map<String, CBroker> registry = new HashMap<>();

  BrokerManager() {

  }

  public void add(CBroker broker) {
    synchronized (registry) {
      if (registry.containsKey(broker.getName()))
        throw new IllegalArgumentException("Broker already exists: " + broker.getName());
      registry.put(broker.getName(), broker);
    }
  }
  
  public void remove(CBroker broker) {
    synchronized (registry) {
      registry.remove(broker.getName());
    }
  }
  
  public CBroker get(String name) {
    synchronized (registry) {
      return registry.get(name);
    }
  }
  
}
