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
import java.util.HashSet;
import java.util.HashMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;
import java.util.Spliterator;
import io.reactivex.functions.Consumer;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.lang.reflect.ParameterizedType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import signalj.timeseries.TimescaleSignal;
import signalj.timeseries.SignalClassInstance;
import signalj.timeseries.Synchronizer;
import signalj.timeseries.PersistentSignal;

public class SetTimescaleSignal<SCISET extends Set<SCI>, SCI extends SignalClassInstance> extends TimescaleSignal<SCISET> implements SetPersistentSignal<SCISET,SCI> {

    private Vector<SCI> contents = null;
    private HashMap<String, Consumer<? extends SCI>> innerSubscribers = new HashMap<String, Consumer<? extends SCI>>();
    private Vector<PersistentSignal> interactionListeners = new Vector<PersistentSignal>();

    public SetTimescaleSignal(String baseName, String name, Synchronizer sync) {
	super(baseName, name, sync);
	for (SCI s : value()) {
	    contents.add(s);
	}
    }

    protected SCISET getValueFromResultSet(ResultSet rs, int column) throws SQLException {
	SCISET retval = (SCISET)(Set<SignalClassInstance>)new HashSet<SignalClassInstance>();
	if (rs.next()) {
	    if (rs.getString(name).equals("__clear__")) return retval;
	    try {
		String[] set = rs.getString(name).split(",");
		for (int i=0; i<set.length; i++) {
		    String[] sep = set[i].split("_");
		    int j;
		    StringBuffer sb = new StringBuffer();
		    sb.append(sep[0]);
		    for (j=1; j<sep.length-1; j++) {
			sb.append(".");
			sb.append(sep[j]);
		    }
		    SCI retInst = null;
		    if (contents != null) {
			for (SCI s : contents) {
			    if (s.getID().equals(set[i])) {
				retInst = s;
				break;
			    }
			}
			if (retInst != null) {
			    retval.add(retInst);
			}
		    } else {
			Constructor c = Class.forName(sb.toString()).getConstructor(String.class);
			c.setAccessible(true);
			SCI sci = (SCI)c.newInstance(sep[sep.length-1]);
			retval.add(sci);
		    }
		}
		if (contents == null) contents = new Vector<SCI>();
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	}
	return retval;
    }

    private Vector<String> getStringArray() {
	Vector<String> retval = new Vector<String>();
	try {
	    String tc = timeCursor == null ? " " : " WHERE time < '" + timeCursor + "'";
	    PreparedStatement stmt = conn.prepareStatement("SELECT " + name + ", time FROM " + dbName + tc + "WHERE " + name + " IS NOT NULL ORDER BY time DESC LIMIT 1");
	    ResultSet rs = stmt.executeQuery();
	    if (rs.next()) {
		String[] sa = rs.getString(name).split(",");
		for (int i=0; i<sa.length; i++) retval.add(sa[i]);
	    }
	    rs.close();
	    stmt.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	return retval;
    }


    public void set(SCI e) {
	sync.startLock();
	sync.setLastAmongTheDepth();
	
	Vector<String> values = getStringArray();
	values.add(e.getID());
	values.remove("__clear__");
	String[] tmp = new String[values.size()];
	for (int i=0; i<values.size(); i++) {
	    tmp[i] = values.elementAt(i);
	}
	//	System.out.println("DEBUG::" + String.join(",",tmp));
	sync.delay(this, "'"+String.join(",",tmp)+"'");
	SCI toBeRemoved = null;
	if (contents == null) contents = new Vector<SCI>();
	for (SCI s : contents) {
	    if (s.getID().equals(e.getID()) && s != e) toBeRemoved = s;
	}
	if (toBeRemoved != null) contents.remove(toBeRemoved);
	contents.add(e);
	for (PersistentSignal ps : interactionListeners) {
	    ps.updateInteractionDownstream(e, "add");
	}
    }

    public boolean add(SCI e) {
	//	System.out.println("DEBUG:: Group:: adding " + e.getID());
	if (!contains(e)) {
	    //	    System.out.println("Yes, " + e.getID() + " being really added");
	    set(e);
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
	sync.startLock();
	sync.setLastAmongTheDepth();
	
	sync.delay(this, "'__clear__'");
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
	SignalClassInstance sci;
	if (!(o instanceof SignalClassInstance)) return false;
	sci = (SignalClassInstance)o;
	if (!contains(sci)) return false;
	Vector<String> values = getStringArray();
	values.remove(sci.getID());
	String[] tmp = new String[values.size()];
	for (int i=0; i<values.size(); i++) {
	    tmp[i] = values.elementAt(i);
	}
	sync.startLock();
	sync.setLastAmongTheDepth();
	
	if (tmp.length == 0) {
	    sync.delay(this, null);
	} else {
	    sync.delay(this, "'"+String.join(",",tmp)+"'");
	}
	SignalClassInstance remval = null;
	for (SCI s : contents) {
	    if (s.getID().equals(sci.getID())) {
		remval = sci;
		break;
	    }
	}
	if (remval != null) contents.remove(remval);
	for (PersistentSignal ps : interactionListeners) {
	    ps.updateInteractionDownstream(remval, "remove");
	}
	return true;
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
	Vector<String> tmp = getStringArray();
	for (Object obj : c) {
	    SignalClassInstance sci = (SignalClassInstance)obj;
	    if (!tmp.contains(sci.getID())) {
		remove(sci);
		retval = true;
	    }
	}
	return retval;
    }
		
    public boolean contains(Object o) {
	SignalClassInstance sci;
	if (!(o instanceof SignalClassInstance)) return false;
	sci = (SignalClassInstance)o;
	Vector<String> sa = getStringArray();
	return sa.contains(sci.getID());
    }

    public boolean containsAll(Collection<?> c) {
	boolean retval = true;
	for (Object sci : c) {
	    if (!contains(sci)) retval = false;
	}
	return retval;
    }

    public boolean isEmpty() {
	return getStringArray().isEmpty();
    }

    public Iterator<SCI> iterator() {
	return value().iterator();
    }

    public int size() {
	return getStringArray().size();
    }
    
    public Object[] toArray() {
	return value().toArray();
    }

    public boolean equals(Object o) {
	SetTimescaleSignal opponent;
	if (!(o instanceof SetTimescaleSignal)) return false;
	opponent = (SetTimescaleSignal)o;
	return getStringArray().equals(opponent.getStringArray());
    }

    // not implemented
    public int hashCode() {
	return -1;
    }

    public <T> T[] toArray(T[] a) {
	if (a == null) throw new NullPointerException();
	Object[] ids = toArray();
	if (a.length < ids.length) a = (T[])Array.newInstance(a.getClass().getComponentType(), a.length);
	for (int i=0; i<a.length; i++) {
	    if (i<ids.length) {
		try {
		    Array.set(a, i, ids[i]);
		} catch (Exception e) {
		    throw new ArrayStoreException();
		}
	    } else {
		a[i] = null;
	    }
	}
	return a;
    }

    public <S extends SCI> void map(Consumer<S> c) {
	try {
	    for (SCI e : value()) {
		c.accept((S)e);
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public SCISET __signalj__get() {
	if (sync.isCausal()) {
	    waitUntilReadable();
	}
	
	if (timeCursor != null) {
	    return valueWithTimeCursor();
	} else {
	    SCISET retval = null;
	    try {
		PreparedStatement stmt = conn.prepareStatement("SELECT " + name + ", time FROM " + dbName + " WHERE " + name + " IS NOT NULL ORDER BY time DESC LIMIT 1");
		ResultSet rs = stmt.executeQuery();
		retval = getValueFromResultSet(rs,1);
		rs.close();
		stmt.close();
	    } catch (SQLException e) {
		e.printStackTrace();
	    }
	    return retval;
	}
    }

    public void setSubscriber(Consumer<? extends SCI> c, String fname) {
	innerSubscribers.put(fname,c);
    }

    public void addInteractionListener(PersistentSignal ps) {
	interactionListeners.add(ps);
    }

}
