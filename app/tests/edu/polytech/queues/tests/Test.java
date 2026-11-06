/*
 * Copyright (C) 2026 Pr. Olivier Gruber                                    
 *                                                                       
 * This program is free software: you can redistribute it and/or modify  
 * it under the terms of the GNU General Public License as published by  
 * the Free Software Foundation, either version 3 of the License, or     
 * (at your option) any later version.                                   
 *                                                                       
 * This program is distributed in the hope that it will be useful,       
 * but WITHOUT ANY WARRANTY; without even the implied warranty of        
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         
 * GNU General Public License for more details.                          
 *                                                                       
 * You should have received a copy of the GNU General Public License     
 * along with this program.  If not, see <http://www.gnu.org/licenses/>. 
 */
package edu.polytech.queues.tests;

import java.lang.reflect.Constructor;

import edu.polytech.queues.Bootstrap;
import edu.polytech.queues.QueueBroker;
import edu.polytech.queues.Task;
import edu.polytech.utils.Executor;

/**
 * This test is a simple echo test based on a client-server architecture. The
 * server accepts connections from clients and echoes whatever message it
 * receives back to the client that sent it.
 * 
 * Two different servers are provided, one not using a pool of tasks to process
 * client sessions and one with a pool of tasks.
 * 
 * Without special arguments, this test uses the following classes for the queue
 * and channel layers:
 * 
 * - info5.sar.channels.CBroker - info5.sar.events.queues.CQueueBroker
 * 
 * But you may change these classes with your own with the arguments
 * -cbroker:my.pkg.MyBroker -qbroker:my.pkg.MyQueueBroker
 * 
 * if your channel broker is implemented by the class my.pkg.MyBroker and your
 * queue broker is implement by the class my.pkg.MyQueueBroker.
 * 
 * You can control the test via arguments given when launching:
 * 
 * -nclients: the number of clients with the argument -nconnects: the number of
 * times a client sequentially connects with the server, each connection being a
 * session. -nbytes: the total number of bytes sent by a client within a
 * session. -pool: controls which server is instanciated, the one with a pool of
 * tasks or not. The default is without using a pool.
 * 
 * At first, start simple with one client, connecting only once, and sending
 * only one message:
 * 
 * -nclients:1 -nconnects:1 -nbytes:100
 * 
 */

public class Test {

  static void ensure(boolean cond) {
    Executor.ensure(cond);
  }

  static void failStop() {
    Executor.failStop();
  }

  static void failStop(Throwable th) {
    Executor.failStop(th);
  }

  private static String QueueBrokerClassName = "edu.polytech.queues.local.Boot";
  private static final String QBROKER_OPTION = "-qbroker:";
  private static final String NCLIENTS_OPTION = "-nclients:";
  private static final String NCONNECTS_OPTION = "-nconnects:";
  private static final String NBYTES_OPTION = "-nbytes:";

  private static void parseArgs(String args[]) {
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if (arg.startsWith(QBROKER_OPTION))
        QueueBrokerClassName = arg.substring(QBROKER_OPTION.length());
      else if (arg.startsWith(NCLIENTS_OPTION))
        nclients = Integer.valueOf(arg.substring(NCLIENTS_OPTION.length()));
      else if (arg.startsWith(NCONNECTS_OPTION))
        nconnects = Integer.valueOf(arg.substring(NCONNECTS_OPTION.length()));
      else if (arg.startsWith(NBYTES_OPTION))
        nbytes = Integer.valueOf(arg.substring(NBYTES_OPTION.length()));
    }
  }

  static void printOptions() {
    System.out.println("--------------------------------------");
    System.out.println("Using Queue Broker: " + QueueBrokerClassName);
    System.out.println("--------------------------------------");
    System.out.println("  nclients=" + nclients);
    System.out.println("  nconnects=" + nconnects);
    System.out.println("  nbytes=" + nbytes);
    System.out.println("--------------------------------------\n\n");
  }

  private static int nclients = 2;
  private static int nconnects = 2;
  private static int nbytes = 1024;

  public static void main(String args[]) throws Exception {
    String name = "Server";
    int port = 80;
    TestClient clients[];
    parseArgs(args);
    printOptions();

    newBootstrap();

    clients = new TestClient[nclients];

    Task.Listener l = new Task.Listener() {

      boolean dead(Task t) {
        for (int i = 0; i < clients.length; i++) {
          if (clients[i] == null)
            continue;
          if (clients[i].task() == t) {
            clients[i] = null;
            nclients--;
            return true;
          }
        }
        return false;
      }

      @Override
      public void failed(Task t, Throwable th) {
        th.printStackTrace(System.err);
        System.err.println("Task "+t.getName()+" failed");
        if (!dead(t))
          System.err.println("PANIC: the failed task does not match any client");
      }

      @Override
      public void completed(Task t, Object o) {
        if (!dead(t)) {
          System.err.println("PANIC: a completed task "+t.getName()+" does not match any client");
          System.exit(-1);
        }
      }
    };


    bootstrap.newTask(() -> {
      Task task = Task.task();
      new TestServer(name,port);
      // not necessary but forces a cross-broker lookup
      // since the server uses its own broker as well.
      QueueBroker broker = task.newBroker("Client Broker");
      for (int i = 0; i < nclients; i++) {
        String cn = "Client" + i;
        clients[i] = new TestClient(broker,i, name, port, nconnects, nbytes);
        clients[i].task().set(l);
      }
    }, "boot");

    while (nclients != 0) {
      try {
        System.out.printf("=== %d clients remaining...\n",nclients);
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // nothing to do...
      }
    }
    System.out.println("\n\nThat's all folks...");
    System.exit(0);
  }

  private static Class cbroker_cls;
  private static Constructor cbroker_ctor;
  private static Class qbroker_cls;
  private static Constructor qbroker_ctor;

  static Class cls;
  static Constructor ctor;
  static Bootstrap bootstrap;

  static void newBootstrap() throws Exception {
    if (cls == null) {
      cls = Class.forName(QueueBrokerClassName);
      Class params[] = new Class[0];
      ctor = cls.getConstructor(params);
    }
    bootstrap = (Bootstrap) ctor.newInstance(new Object[0]);
  }

}
