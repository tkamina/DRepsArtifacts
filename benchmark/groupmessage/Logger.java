package groupmessage;

import signalj.timeseries.annotation.*;

@eventual
signal class Logger {
    signal Set<User> users = User.allInstances();
    //interaction String log(Set<User> users$String$u$inbox) {
    //	return "message: " + u.getID() + " received " + u.inbox;
    //}
    interaction String log[users.u:String.inbox] = "message: " + u.getID() + " received " + u.inbox;
    public Logger(String id) { }
}
