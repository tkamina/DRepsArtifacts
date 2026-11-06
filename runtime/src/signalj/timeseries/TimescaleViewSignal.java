/* Copyright (c) 2019-2024, Tetsuo Kamina
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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;

abstract class TimescaleViewSignal<T> extends TimescaleSignal<T> {

    protected String preparedQuery;
    protected String columnName;
    protected String fromDB;

    TimescaleViewSignal(String baseName, String name, String fromName, String columnName) {
	this.baseName = baseName;
	this.name = name;
	this.dbName = baseName + "_" + name;
	this.columnName = columnName;
	String additional = columnName==null ? "_" + fromName : "";
	this.fromDB = baseName + additional;
	connectDB(baseName);
    }
    
    protected String createView() {
	return "CREATE VIEW " + dbName + " AS " + preparedQuery;
    }

    protected String postfix() { return " ORDER BY time LIMIT 1"; }

    private boolean isTimeseriesData() {
	try {
	    PreparedStatement stmt = conn.prepareStatement("SELECT column_name FROM information_schema.columns WHERE table_name='" + dbName.toLowerCase() + "'");
	    ResultSet res = stmt.executeQuery();
	    while (res.next()) {
		if (res.getString("column_name").equals("time"))
		    return true;
	    }
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	return false;
    }

    public T __signalj__get() {
	if (timeCursor == null || !isTimeseriesData()) {
	    PreparedStatement stmt;
	    ResultSet res = null;
	    ResultSetMetaData rsmd = null;
	    T retval = null;
	    try {
		stmt = conn.prepareStatement("SELECT value FROM " + dbName + postfix());
		res = stmt.executeQuery();
		retval = getValueFromResultSet(res, 1);
		res.close();
		stmt.close();
	    } catch (SQLException e) {
		e.printStackTrace();
	    }
	    return retval;
	} else {
	    return valueWithTimeCursor();
	}
    }

    protected T valueWithTimeCursor() {
	T retval = null;
	try {
	    PreparedStatement stmt = conn.prepareStatement("SELECT value, time FROM " + dbName + " WHERE time < '" + timeCursor + "' ORDER BY time DESC LIMIT 1");
	    ResultSet rs = stmt.executeQuery();
	    retval = getValueFromResultSet(rs,1);
	    rs.close();
	    stmt.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	return retval;
    }

    public T value() { return __signalj__get(); }

    public PersistentSignal<T> within(Timestamp ts, String interval, String name) {
	return TimescaleFactory.<T>newWithin(ts, interval, baseName, name, this.name, null);
    }

    public PersistentSignal<T> lastDiff(int offset, String name) {
	return TimescaleFactory.<T>newLastDiff(offset, baseName, name, this.name, null);
    }

    public PersistentSignal<Double> distance(PersistentSignal p, String name) {
	return TimescaleFactory.newDistance(p, baseName, name, this.name, null);
    }

    public PersistentSignal<Integer> pcount(String name) {
	return TimescaleFactory.<Integer>newAnalytic("count", baseName, name, this.name, null);
    }

    public PersistentSignal<Double> avg(String name) {
	return TimescaleFactory.<Double>newAnalytic("avg", baseName, name, this.name, null);
    }

    public PersistentSignal<T> psum(String name) {
	return TimescaleFactory.<T>newAnalytic("sum", baseName, name, this.name, null);
    }

    public PersistentSignal<T> max(String name) {
	return TimescaleFactory.<T>newAnalytic("max", baseName, name, this.name, null);
    }

    public PersistentSignal<T> min(String name) {
	return TimescaleFactory.<T>newAnalytic("min", baseName, name, this.name, null);
    }

    public PersistentSignal<T> first(String name) {
	return TimescaleFactory.<T>newFirst(baseName, name, this.name, null);
    }

    public void dbgen() { //throws DoubleInstanceException {
	PreparedStatement stmt = null, insertInstance = null;
	try {
	    if (!Timeseries.checkDB(dbName,conn)) {
		conn.setAutoCommit(false);
		System.out.println("DEBUG: " + createView());
		stmt = conn.prepareStatement(createView());
		insertInstance = conn.prepareStatement("INSERT INTO persistent_instances VALUES ('" + dbName.toLowerCase() + "', 'true')");
		stmt.executeUpdate();
		insertInstance.executeUpdate();
		stmt.close();
		insertInstance.close();
		conn.commit();
	    } else {
		PreparedStatement update, hasCreated = conn.prepareStatement("SELECT active FROM persistent_instances WHERE relname = '" + dbName.toLowerCase() + "'");
		ResultSet rs = hasCreated.executeQuery();
		if (rs.next()) {
		    if (rs.getBoolean(1)) {
			//			throw new DoubleInstanceException("duplicated persistent signal instance: " + name);
		    } else {
			update = conn.prepareStatement("UPDATE persistent_instances SET active = 'true' WHERE relname = '" + dbName.toLowerCase() + "'");
			update.executeUpdate();
			update.close();
		    }
		}
		hasCreated.close();
	    }
	} catch (SQLException e) {
	    e.printStackTrace();
	}
    }

    protected String tableType() { return "VIEW"; }

    // override to make this method no-effect on view signals
    public void addExternSync(Synchronizer sync) { }
    
}
