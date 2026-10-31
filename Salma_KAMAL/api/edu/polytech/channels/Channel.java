package edu.polytech.channels;

public interface Channel {
	int read(byte bytes[], int offset, int length);

	int write(byte bytes[], int offset, int length);

	boolean disconnected();

	void disconnect();
}
