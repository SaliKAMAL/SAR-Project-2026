package edu.polytech.channels;

public interface Broker {
	String getName();

	Channel connect(String name, int port);

	Channel accept(int port);
}
