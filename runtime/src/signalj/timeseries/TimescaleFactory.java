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

package signalj.timeseries;

import java.util.Set;
import java.sql.Timestamp;
import signalj.timeseries.collection.SetTimescaleSignal;
import signalj.timeseries.collection.SetPersistentSignal;

public class TimescaleFactory {
    
    public static <T> PersistentSignal<T> newPersistent(String baseName, String name, Synchronizer sync) {
	return new TimescaleSignal<T>(baseName, name, sync);
    }

    public static <S extends Set<T>, T extends SignalClassInstance> SetPersistentSignal<S,T> newSetPersistent(String baseName, String name, Synchronizer sync) {
	return new SetTimescaleSignal<S,T>(baseName, name, sync);
    }

    public static <T> PersistentSignal<T> newWithin(Timestamp ts, String interval, String baseName, String name, String fromName, String columnName) {
	return new WithinSignalTimescale<T>(ts,interval,baseName,name,fromName,columnName);
    }

    public static <T> PersistentSignal<T> newLastDiff(int offset, String baseName, String name, String fromName, String columnName) {
	return new LastDiffSignalTimescale<T>(offset,baseName,name,fromName,columnName);
    }

    public static PersistentSignal<Double> newDistance(PersistentSignal p, String baseName, String name, String fromName, String columnName) {
	return new DistanceSignalTimescale(p, baseName, name, fromName, columnName);
    }

    public static <T> PersistentSignal<T> newAnalytic(String opName, String baseName, String name, String fromName, String columnName) {
	return new AnalyticSignalTimescale<T>(opName, baseName, name, fromName, columnName);
    }

    public static <T> PersistentSignal<T> newFirst(String baseName, String name, String fromName, String columnName) {
	return new FirstSignalTimescale<T>(baseName, name, fromName, columnName);
    }

}
