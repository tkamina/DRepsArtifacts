package signalj;

public class ObjectSignal {

    private Signal s;

    void setSignal(Signal s) { this.s = s; }

    public void push() {
	s.set(this);
    }

}
