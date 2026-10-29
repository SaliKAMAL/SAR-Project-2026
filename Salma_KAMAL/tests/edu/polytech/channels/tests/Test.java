/*
 * Copyright (C) 2023 Pr. Olivier Gruber                                    
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
package edu.polytech.channels.tests;

import java.lang.reflect.Constructor;

import edu.polytech.channels.Bootstrap;
import edu.polytech.channels.Broker;
import edu.polytech.channels.Task;

public class Test {

  static void ensure(boolean cond) {
    if (!cond)
      failStop();
  }

  static void failStop() {
    System.err.println("FAIL-STOP: suicide.");
    System.exit(-1);
  }

  static void failStop(Throwable th) {
    th.printStackTrace(System.err);
    failStop();
  }

  static String ClassName = "edu.polytech.channels.local.Boot";
  static final String BROKER_OPTION = "-broker:";
  static final String NCLIENTS_OPTION = "-nclients:";
  static final String NCONNECTS_OPTION = "-nconnects:";
  static final String NBYTES_OPTION = "-nbytes:";
  static final String POOL_OPTION = "-pool";

  static void parseArgs(String args[]) {
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if (arg.startsWith(BROKER_OPTION))
        ClassName = arg.substring(BROKER_OPTION.length());
      else if (arg.startsWith(NCLIENTS_OPTION))
        nclients = Integer.valueOf(arg.substring(NCLIENTS_OPTION.length()));
      else if (arg.startsWith(NCONNECTS_OPTION))
        nconnects = Integer.valueOf(arg.substring(NCONNECTS_OPTION.length()));
      else if (arg.startsWith(NBYTES_OPTION))
        nbytes = Integer.valueOf(arg.substring(NBYTES_OPTION.length()));
      else if (arg.equals(POOL_OPTION))
        pool = true;
    }
  }

  static void printOptions() {
    System.out.println("--------------------------------------");
    System.out.println("Using Broker: " + ClassName);
    System.out.println("--------------------------------------");
    System.out.println("  nclients=" + nclients);
    System.out.println("  nconnects=" + nconnects);
    System.out.println("  nbytes=" + nbytes);
    System.out.println("--------------------------------------\n\n");
  }

  static boolean pool;
  static int nclients = 2;
  static int nconnects = 2;
  static int nbytes = 1024;
  static Bootstrap bootstrap;

  public static void main(String args[]) throws Exception {
    String name = "Server";
    int port = 80;
    Task tcs[];

    parseArgs(args);
    printOptions();

    newBootstrap();

    createServer(name, port);
    tcs = createClients(name, port);
    waitForClients(tcs);
    System.out.println("\n\nThat's all folks...");
    System.exit(0);
  }

  private static void createServer(String name, int port) throws Exception {
    Task ts;
    TestServer s;
    TestClient c;
    Broker b = bootstrap.newBroker(name);
    Runnable r;
    if (pool)
      r = new TestPoolServer(port);
    else
      r = new TestServer(port);
    ts = bootstrap.newTask(b, r, name);
  }

  private static Task[] createClients(String name, int port) throws Exception {
    Task tcs[] = new Task[nclients];
    TestClient c;
    Broker b;
    Runnable r;
    Task t;
    for (int i = 0; i < nclients; i++) {
      String cn = "Client" + i;
      b = bootstrap.newBroker(cn);
      r = new TestClient(i, name, port, nconnects, nbytes);
      tcs[i] = bootstrap.newTask(b, r, name);
    }
    return tcs;
  }

  private static void waitForClients(Task tcs[]) {
    while (true) {
      int ntasks = 0;
      for (int i = 0; i < nclients; i++) {
        Task tc = tcs[i];
        if (tc != null && !tc.dead())
          ntasks++;
      }
      if (ntasks == 0)
        break;
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // nothing to do...
      }
    }
  }

  static Class cls;
  static Constructor ctor;

  static void newBootstrap() throws Exception {
    if (cls == null) {
      cls = Class.forName(ClassName);
      Class params[] = new Class[0];
      ctor = cls.getConstructor(params);
    }
    bootstrap = (Bootstrap) ctor.newInstance(new Object[0]);
  }

}
