package groupmessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import signalj.timeseries.DoubleInstanceException;

class Writer implements Runnable {
    int maxMessage = 10;
    Group g1;

    Writer(Group g) { g1 = g; }
    
    public void run() {
	for (int i=0; i<maxMessage; i++) {
	    g1.post("Message " + i);
	}
    }
}

class Reader implements Runnable {
    int id;

    Reader(int id) { this.id = id; }

    public void run() {
	try {
	    User u = new User("u" + id);
	    long start = System.nanoTime();
	    String dummy = u.inbox;
	    long end = System.nanoTime();
	    System.out.println(end - start);
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }
}
	    
public class Exp2 {
    public static void main(String[] args) {
	int maxUser = 100;
	
	try {
	    Group g1 = new Group("exp1");
	    Logger logger = new Logger("logger1");
	    g1.clearUsers();
	    for (int i=0; i<maxUser; i++) {
		g1.addUser(new User("u" + i));
	    }
	    Thread writer = new Thread(new Writer(g1));
	    writer.start();
	    
	    Thread.sleep(1000);
	    
	    List<Integer> list = new ArrayList<>();
	    for (int i=0; i<=100; i++) { list.add(i); }
	    Collections.shuffle(list);
	    List<Integer> selected = list.subList(0, 10);
	    for (Integer i : selected) {
		Thread r = new Thread(new Reader(i));
		r.start();
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }
}
