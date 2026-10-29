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

import edu.polytech.channels.Broker;
import edu.polytech.channels.Channel;
import edu.polytech.channels.Task;

public class TestClient implements Runnable {

  static boolean VERBOSE = true;
  static void log(String s) {
    if (VERBOSE)
      System.out.println(s);
  }
  
  class _Reader implements Runnable {
    Channel ch;
    int nbytes;

    _Reader(Channel ch, int nbytes) {
      this.ch = ch;
      this.nbytes = nbytes;
    }

    @Override
    public void run() {
      try {
        read();
      } finally {
        log(name + ":Reader: done.");
      }
    }

    private void read() {
      byte bytes[] = new byte[nbytes];
      int offset = 0;
      int remaining = bytes.length;
      while (remaining != 0 && !ch.disconnected()) {
        int n = ch.read(bytes, offset, remaining);
        log(name + ":Reader: read " + n + " bytes");
        Test.ensure(n >= 0);
        if (n == 0) Test.ensure(ch.disconnected());
        offset += n;
        remaining -= n;
        log(name + ":Reader: waiting for " + remaining + " bytes");
      }
      log(name + ":Reader: checking " + offset + " bytes");
      for (int i = 0; i < bytes.length; i++)
        if (bytes[i] != (byte) i)
          throw new Error("Reader["+name+"] bytes[i]="+bytes[i]+" != "+(byte)i);
      log(name + ":Reader: done");
    }
  }

  class _Writer implements Runnable {
    Channel ch;
    int nbytes;

    _Writer(Channel ch, int nbytes) {
      this.ch = ch;
      this.nbytes = nbytes;
    }

    @Override
    public void run() {
      try {
        write();
      } finally {
        log(name + ":Writer: done.");
      }
    }

    private void write() {
      byte bytes[] = new byte[nbytes];
      for (int i = 0; i < bytes.length; i++)
        bytes[i] = (byte) i;

      int offset = 0;
      int remaining = bytes.length;
      while (remaining != 0 && !ch.disconnected()) {
        int n = ch.write(bytes, offset, remaining);
        log(name + ":Writer: wrote " + n + " bytes");
        Test.ensure(n >= 0);
        if (n == 0) Test.ensure(ch.disconnected());
        offset += n;
        remaining -= n;
      }
    }
  }

  Task reader, writer;
  String connectName;
  int connectPort;
  int nconnects;
  int nbytes;
  int clientNo;
  String name;
  Broker broker;

  TestClient(int no, String name, int port, int nconnects, int nbytes) {
    this.clientNo = no;
    this.connectName = name;
    this.connectPort = port;
    this.nconnects = nconnects;
    this.nbytes = nbytes;
  }

  @Override
  public void run() {
    Task task = Task.task();
    name = "Client[" + clientNo + "]";
    System.out.println(name + ": started!");
    broker = task.getBroker();
    for (int i = 0; i < nconnects; i++)
      session(task);
    System.out.println(name + ": done.");
  }

  private void session(Task task) {
    Channel ch = null;
    Runnable r;
    while (ch == null) {
      ch = broker.connect(connectName, connectPort);
      if (ch == null)
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          // nothing to do...
        }
    }
    r = new _Writer(ch, nbytes);
    reader = task.newTask(broker, r, name + ":Reader");
    r = new _Reader(ch, nbytes);
    writer = task.newTask(broker, r, name + ":Writer");
    
    while (reader.alive() || writer.alive()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        // nothing to do...
      }
    }
    ch.disconnect();
  }
}
