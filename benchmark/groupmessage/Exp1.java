package groupmessage;

import signalj.timeseries.DoubleInstanceException;

public class Exp1 {
    public static void main(String[] args) {
	int maxUser = 1;
	int maxMessage = 10;
	
	try {
	    Group g1 = new Group("exp1");
	    Logger logger = new Logger("logger1");
	    //	    Thread.sleep(1000);
	    g1.clearUsers();
	    for (int i=0; i<maxUser; i++) {
		g1.addUser(new User("u" + i));
	    }
	    long start = System.nanoTime();
	    for (int i=0; i<maxMessage; i++) {
		g1.post("Message " + i);
	    }
	    long end = System.nanoTime();
	    System.out.println(end - start);
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }
}
