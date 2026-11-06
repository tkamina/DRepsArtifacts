/* Copyright (c) 2019-2025, Tetsuo Kamina
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package signalj.timeseries.collection;

import java.util.Set;
import java.util.HashMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;
import java.lang.reflect.Field;
import io.reactivex.functions.Consumer;
//import java.util.function.Consumer;
import java.lang.reflect.Array;
import signalj.timeseries.SignalClassInstance;
import java.sql.Timestamp;
import java.sql.Connection;
import signalj.timeseries.Synchronizer;
import signalj.timeseries.SignalClassInstance;
import signalj.timeseries.PersistentSignal;
import signalj.timeseries.DoubleInstanceException;

public class SetSignal<SCISET extends Set<SCI>, SCI extends SignalClassInstance> implements SetPersistentSignal<SCISET,SCI> {

    private Vector<SCI> contents = new Vector<SCI>();
    //private HashMap<String, Consumer<? extends SCI>> innerSubscribers = new HashMap<String, Consumer<? extends SCI>>();
    private Vector<Consumer> subscribers = new Vector<Consumer>();

    private SignalClassInstance signalClassInst;
    private Vector<PersistentSignal> interactionListeners = new Vector<PersistentSignal>();
    private boolean listenInteraction = false;
    private Vector<SignalClassInstance> interactionDownstreams = new Vector<SignalClassInstance>();
    private Vector<SetPersistentSignal> interactionDownstreamSetSignals = new Vector<SetPersistentSignal>();

    // BEGIN : 12/10
    private Vector<String> fnames = new Vector<String>();
    // END

    public SetSignal() {
    	interactionListeners.add(this);
    }

    private void setSubscriberInner(SCI s) {
	//	for (String fname : innerSubscribers.keySet()) {
	    try {
		for (Consumer c : subscribers) {
		    c.accept(s);
		}
		/*
		Consumer<? extends SCI> c = innerSubscribers.get(fname);
		Field f = s.getClass().getField(fname);
		f.setAccessible(true);
		System.out.println("consumer " + c);
		((PersistentSignal)f.get(s)).psubscribe(c);
		*/
	    } catch (Exception e) {
		System.err.println("Adding subscriber is failed on " + s.getID());
		e.printStackTrace();
	    }
	    //	}
    }
    
    public boolean add(SCI e) {
	if (!contains(e)) {
	    contents.add(e);
	    setSubscriberInner(e);
	    // BEGIN : 12/10
	    //	    for (PersistentSignal ps : interactionListeners) {
	    //		ps.updateInteractionDownstream(e, "add");
	    //	    }
	    for (SignalClassInstance sci : interactionDownstreams) {
		for (String fname : fnames) {
		    try {
			Field f = e.getClass().getField(fname);
			f.setAccessible(true);
			((PersistentSignal)f.get(e)).setInteractionDownstream(sci);
		    } catch (Exception ex) {
			ex.printStackTrace();
		    }
		}
	    }
	    // END
	    return true;
	}
	return false;
    }

    public boolean addAll(Collection<? extends SCI> c) {
	boolean retval = false;
	for (SCI sci : c) {
	    if (add(sci)) retval = true;
	}
	return true;
    }

    public void clear() {
	if (contents != null) {
	    for (PersistentSignal ps : interactionListeners) {
		for (SCI sci: contents) {
		    ps.updateInteractionDownstream(sci, "remove");
		}
	    }
	}
	contents =  new Vector<SCI>();
    }

    public boolean remove(Object o) {
	for (PersistentSignal ps : interactionListeners) {
	    ps.updateInteractionDownstream((SignalClassInstance)o, "remove");
	}
	return contents.remove(o);
    }

    public boolean removeAll(Collection<?> c) {
	boolean retval = false;
	for (Object sci : c) {
	    if (remove(sci)) retval = true;
	}
	return retval;
    }

    public boolean retainAll(Collection<?> c) {
	boolean retval = false;
	for (Object obj : c) {
	    if (!contents.contains(obj)) {
		remove(obj);
		retval = true;
	    }
	}
	return retval;
    }
		
    public boolean contains(Object o) {
	return contents.contains(o);
    }

    public boolean containsAll(Collection<?> c) {
	boolean retval = true;
	for (Object sci : c) {
	    if (!contains(sci)) retval = false;
	}
	return retval;
    }

    public boolean isEmpty() {
	return contents.isEmpty();
    }

    public Iterator<SCI> iterator() {
	return contents.iterator();
    }

    public int size() {
	return contents.size();
    }
    
    public Object[] toArray() {
	return contents.toArray();
    }

    public boolean equals(Object o) {
	SetSignal opponent;
	if (!(o instanceof SetSignal)) return false;
	opponent = (SetSignal)o;
	return contents.equals(opponent.contents);
    }

    // not implemented
    public int hashCode() {
	return -1;
    }

    public <T> T[] toArray(T[] a) {
	return contents.toArray(a);
    }

    public <S extends SCI> void map(Consumer<S> c) {
	try {
	    for (SCI e : contents) {
		c.accept((S)e);
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public void setSubscriber(Consumer<? extends SCI> c, String fname) {
	subscribers.add(c);
	// BEGIN: 12/10
	fnames.add(fname);
	// END
	//	innerSubscribers.put(fname,c);
    }

    public String name() { return null; }

    public String dbName() { return null; }

    public PersistentSignal<SCISET> setName(String name) { return null; }

    public PersistentSignal<SCISET> within(Timestamp ts, String interval, String name) { return null; }

    public PersistentSignal<SCISET> lastDiff(int offset, String name) { return null; }

    public PersistentSignal<Integer> pcount(String name) { return null; }

    public PersistentSignal<Double> avg(String name) { return null; }

    public PersistentSignal<SCISET> first(String name) { return null; }

    public PersistentSignal<SCISET> psum(String name) { return null; }

    public PersistentSignal<SCISET> max(String name) { return null; }

    public PersistentSignal<SCISET> min(String name) { return null; }

    public PersistentSignal<Double> distance(PersistentSignal p, String name) { return null; }

    public SCISET value() { return null; }

    public SCISET __signalj__get() { return null; }

    public void set(SCISET val) { }
    public void setUpstream(SignalClassInstance o, SignalClassInstance n) { }

    public void dbgen() throws DoubleInstanceException { }

    public void discard() { }

    public void resume() { }

    public void reset() { }

    public boolean isInitialized() { return false; }

    public void setIfNotInitialized(SCISET val) { }

    public void snapshot(Timestamp ts) { }

    public Timestamp firstTimestamp() { return null; }

    public Timestamp latestTimestamp() { return null; }

    public void pushSignalClassInst() { }
    public void setSignalClassInst(SignalClassInstance signalClassInst) {
	this.signalClassInst = signalClassInst;
    }

    public <S extends SCISET> void psubscribe(Object c) { }
    public void effect() { }

    public Vector<Timestamp> timestampList() { return null; }

    public void addExternSync(Synchronizer sync) { }

    public void ensureConsistency(Timestamp lastCheckPoint, Timestamp currentCheckPoint) { }

    public void updateSwitchHistory() { }

    public void block() { }
    public void unblock() { }
    public boolean isBlocked() { return false; }
    public void setConnection(Connection conn) { }
    public void effect(Timestamp t) { }
    public SignalClassInstance getSignalClassInst() { return null; }

    public Timestamp getImprocessingTimestamp() { return null; }

    public void addDown(Synchronizer down) { }
    public Vector<Synchronizer> getDownstreams() { return null; }

    public boolean listenInteraction() {
	return listenInteraction;
    }
    public void setListenInteraction(boolean v) {
	listenInteraction = v;
    }

    public void setInteractionDownstream(SignalClassInstance sci) {
	System.out.println("SetSignal: Adding " + sci.getID());
	interactionDownstreams.add(sci);
	sci.getSynchronizer().setInteractionUpstream(this.signalClassInst);
    }

    public void removeInteractionDownstream(SignalClassInstance sci) {
	interactionDownstreams.remove(sci);
	sci.getSynchronizer().removeInteractionUpstream(this.signalClassInst);
    }
    
    public void setInteractionDownstream(SetPersistentSignal sps) {
	interactionDownstreamSetSignals.add(sps);
	sps.addInteractionListener(this);
	for (Object sci : sps) {
	    setInteractionDownstream((SignalClassInstance)sci);
	}
    }

    public void addInteractionListener(PersistentSignal ps) {
	interactionListeners.add(ps);
    }
    public void updateInteractionDownstream(SignalClassInstance sci, String op) {
	if (op.equals("remove")) {
	    removeInteractionDownstream(sci);
	} else if (op.equals("add")) {
	    setInteractionDownstream(sci);
	}
    }


}
