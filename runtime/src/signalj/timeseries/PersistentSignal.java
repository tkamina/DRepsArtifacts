/* Copyright (c) 2019-2026, Tetsuo Kamina
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

package signalj.timeseries;

import java.sql.Timestamp;
import java.sql.Connection;
import java.util.Vector;

import signalj.timeseries.collection.SetPersistentSignal;

public interface PersistentSignal<T> {

    public String name();

    public String dbName();

    public PersistentSignal<T> setName(String name);

    public PersistentSignal<T> within(Timestamp ts, String interval, String name);

    public PersistentSignal<T> lastDiff(int offset, String name);

    public PersistentSignal<Integer> pcount(String name);

    public PersistentSignal<Double> avg(String name);

    public PersistentSignal<T> first(String name);

    public PersistentSignal<T> psum(String name);

    public PersistentSignal<T> max(String name);

    public PersistentSignal<T> min(String name);

    public PersistentSignal<Double> distance(PersistentSignal p, String name);

    public T value();

    public T __signalj__get();

    public void set(T val);
    public void setUpstream(SignalClassInstance o, SignalClassInstance n);

    public void dbgen() throws DoubleInstanceException;

    public void discard();

    public void resume();

    public void reset();

    public boolean isInitialized();

    public void setIfNotInitialized(T val);

    public void snapshot(Timestamp ts);

    public Timestamp firstTimestamp();

    public Timestamp latestTimestamp();

    public void pushSignalClassInst();
    public void setSignalClassInst(SignalClassInstance signalClassInst);
    public SignalClassInstance getSignalClassInst();

    public <S extends T> void psubscribe(Object c);
    public void effect(Timestamp ts);

    public Vector<Timestamp> timestampList();

    public void addExternSync(Synchronizer sync);

    public void ensureConsistency(Timestamp lastCheckPoint, Timestamp currentCheckPoint);

    public void updateSwitchHistory();

    public void block();
    public void unblock();
    public boolean isBlocked();

    public Timestamp getImprocessingTimestamp();

    public void addDown(Synchronizer down);
    public Vector<Synchronizer> getDownstreams();

    public boolean listenInteraction();
    public void setListenInteraction(boolean v);

    public void setInteractionDownstream(SignalClassInstance sci);
    public void setInteractionDownstream(SetPersistentSignal sps);
    public void updateInteractionDownstream(SignalClassInstance obj, String op);

    public void setConnection(Connection conn);
}
