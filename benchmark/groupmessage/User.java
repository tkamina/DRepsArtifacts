package groupmessage;

import signalj.timeseries.annotation.*;

//@causal
@serializable
signal class User {
    public persistent signal String inbox;
    public User(String id) { }
}
