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

public class TestPoolServer implements Runnable {
	static boolean VERBOSE = false;

	static void log(String s) {
		if (VERBOSE)
			System.out.println(s);
	}

	static void log(Throwable th) {
		if (VERBOSE)
			th.printStackTrace();
	}

	Broker broker;
	int port;
	_Worker[] workers;
	static final int POOL_SIZE = 16;

	TestPoolServer(int port) {
		this.port = port;
	}

	private void createPool() {
		Task task = (Task) Thread.currentThread();
		broker = task.getBroker();
		workers = new _Worker[POOL_SIZE];
		for (int no = 0; no < workers.length; no++) {
			String name = broker.getName() + ":Worker[" + no + "]";
			workers[no] = new _Worker(name);
		}
	}

	private _Worker grabWorker() {
		while (true) {
			for (int no = 0; no < workers.length; no++) {
				_Worker w = workers[no];
				if (w.available())
					return w;
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException ex) {
				// nothing to do here.
			}
		}
	}

	@Override
	public void run() {
		int cno = 0;
		Channel ch;
		System.out.println("Server started!");
		try {
			createPool();
			while (true) {
				_Worker w = grabWorker();
				ch = broker.accept(port);
				w.request(ch);
			}
		} finally {
			System.out.println("Server done.");
		}
	}

	class _Worker implements Runnable {
		String name;
		Task task;
		Channel ch;

		_Worker(String name) {
		  Task t = Task.task();
			task = t.newTask(broker,this,name);
		}

		boolean available() {
			return ch == null;
		}

		synchronized void request(Channel ch) {
			this.ch = ch;
			notify();
		}

		private void sleep() {
			while (ch == null) {
				try {
					wait();
				} catch (InterruptedException ex) {
					// nothing to do here.
				}
			}
		}

		@Override
		public void run() {
			log(name + " new request.");
			try {
				while (true) {
					synchronized (this) {
						sleep();
						_request(ch);
						ch = null;
					}
				}
			} catch (Throwable th) {
				// what should we do here?
				// There is no rationale for an unknown exception,
				// so let's be fail-stop.
				Test.failStop(th);
			}
		}

		private void _request(Channel ch) {
			log(name + " new request.");
			try {
				byte bytes[] = new byte[128];
				this.ch = ch;
				while (true) {
					int n = read(bytes);
					write(bytes, n);
					log(name + " echoed " + n + " bytes!");
				}
			} finally {
				log(name + " request done.");
			}
		}

		private void write(byte[] bytes, int nbytes) {
			int offset = 0;
			int remaining = nbytes;
			while (remaining != 0 && !ch.disconnected()) {
				int n = ch.write(bytes, offset, remaining);
				Test.ensure(n >= 0);
				if (n == 0)
					Test.ensure(ch.disconnected());
				log(name + " wrote " + n + " bytes!");
				offset += n;
				remaining -= n;
			}
		}

		private int read(byte[] bytes) {
			if (ch.disconnected())
				return 0;
			int n = ch.read(bytes, 0, bytes.length);
			Test.ensure(n >= 0);
			if (n == 0)
				Test.ensure(ch.disconnected());
			return n;
		}
	}

}
