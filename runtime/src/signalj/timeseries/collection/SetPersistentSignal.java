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
import java.util.Collection;
import java.util.Spliterator;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Vector;
import io.reactivex.functions.Consumer;
//import java.util.function.Consumer;
import signalj.timeseries.PersistentSignal;
import signalj.timeseries.SignalClassInstance;

public interface SetPersistentSignal<S,T> extends PersistentSignal<S>, Set<T> {

    public boolean add(T e);
    public boolean addAll(Collection<? extends T> c);
    public void clear();
    public boolean contains(Object o);
    public boolean containsAll(Collection<?> c);
    public boolean equals(Object o);
    public int hashCode();
    public boolean isEmpty();
    public Iterator<T> iterator();
    public boolean remove(Object o);
    public boolean removeAll(Collection<?> c);
    public boolean retainAll(Collection<?> c);
    public int size();
    default public Spliterator<T> spliterator() { return null; }
    public Object[] toArray();
    public <E> E[] toArray(E[] a);
    public <V extends T> void map(Consumer<V> c);
    public void setSubscriber(Consumer<? extends T> c, String fname);
    public void addInteractionListener(PersistentSignal ps);

}
