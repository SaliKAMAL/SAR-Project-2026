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

public class TestServer implements Runnable {
  static boolean VERBOSE = true;
  static void log(String s) {
    if (VERBOSE)
      System.out.println(s);
  }
  static void log(Throwable th) {
    if (VERBOSE)
      th.printStackTrace();
  }

  int port;

  TestServer(int port) {
    this.port = port;
  }

  @Override
  public void run() {
    int cno = 0;
    Channel ch;
    System.out.println("Server started!");
    try {
      Task task = Task.task();
      Broker broker = task.getBroker();
      while (true) {
        ch = broker.accept(port);
        String name = broker.getName() + ":Worker[" + cno + "]";
        Runnable r= new _Client(ch, cno++);
        Task client = task.newTask(broker,r,name);
      }
    } finally {
      System.out.println("Server done.");
    }
  }

  class _Client implements Runnable {
    Channel ch;
    int no;

    _Client(Channel ch, int no) {
      this.ch = ch;
      this.no = no;
    }

    @Override
    public void run() {
      log("Server: worker[" + no + "] started.");
      try {
        byte bytes[] = new byte[128];
        while (!ch.disconnected()) {
          int n = read(bytes);
          write(bytes, n);
          log("Server: worker[" + no + "] echoed " + n + " bytes!");
        }
      } catch (Throwable th) {
        // what should we do here? 
        // There is no rationale for an unknown exception,
        // so let's be fail-stop.
        Test.failStop(th);
      }
      log("Server: worker[" + no + "] done.");
    }

    private void write(byte[] bytes, int nbytes) {
      int offset = 0;
      int remaining = nbytes;
      while (remaining != 0) {
        int n = ch.write(bytes, offset, remaining);
        Test.ensure(n >= 0);
        if (n == 0) Test.ensure(ch.disconnected());
        log("Server: worker[" + no + "] wrote " + n + " bytes!");
        offset += n;
        remaining -= n;
      }
    }

    private int read(byte[] bytes) {
      int n = ch.read(bytes, 0, bytes.length);
      Test.ensure(n >= 0);
      if (n == 0) Test.ensure(ch.disconnected());
      return n;
    }
  }

}
