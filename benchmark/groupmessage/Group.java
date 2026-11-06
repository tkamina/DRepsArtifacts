package groupmessage;

import io.reactivex.functions.Consumer;
import signalj.timeseries.DoubleInstanceException;
import signalj.timeseries.annotation.*;

@pmode("union") @serializable
public signal class Group {
    public persistent signal Set<User> users;
    public persistent signal String post;

    interaction void onPosting[post] = users.map((Consumer<User>)(u -> u.inbox = post));

    public void addUser(User user) {
	System.out.println("Adding " + user.getID());
	users.add(user);
	System.out.println("Added " + user.getID());
    }
    public void removeUser(User user) {
	users.remove(user);
    }
    public void clearUsers() {
	users.clear();
    }
    public void post(String message) {
	post = message;
    }
    
    public Group(String id) { }

    public static void main(String[] args) {
	try {
	    Group g1 = new Group("g1");
	    Logger logger = new Logger("logger");
	    User kamina = new User("kamina");
	    User suzuki = new User("suzuki");
	    User shinoda = new User("teraoka");
	    g1.clearUsers();
	    g1.addUser(kamina);
	    g1.addUser(suzuki);
	    g1.post("Message 1");
	    g1.addUser(kamina);
	    g1.removeUser(kamina);
	    g1.addUser(shinoda);
	    g1.post("Message 2");
	} catch(DoubleInstanceException e) {
	    e.printStackTrace();
	}
    }
}
