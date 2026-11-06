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

import java.sql.*;
import java.util.StringJoiner;
import java.util.Arrays;
import java.util.HashMap;
import signalj.timeseries.collection.SetSignal;

public class Timeseries {
    // Just a dummy field
    public static Timestamp now = new Timestamp(0);

    private static HashMap<String, SignalClassInstance> instances =
	new HashMap<String, SignalClassInstance>();

    private static HashMap<String, SetSignal> allInstanceMap =
	new HashMap<String, SetSignal>();

    static {
	//	resetPersistentSignals();
    }

    public static void resetPersistentSignals() {
	try {
	    Class.forName("org.postgresql.Driver");
	    Connection conn = DriverManager.getConnection(DBConfig.url,
							  DBConfig.user,
							  DBConfig.password);
	    PreparedStatement stmt = conn.prepareStatement("UPDATE persistent_instances SET active = 'false'");
	    stmt.executeUpdate();
	    stmt.close();
	    conn.close();
	} catch(Exception e) {
	    e.printStackTrace();
	}
    }

    public static boolean checkDB(String name, Connection conn) {
	try {
	    PreparedStatement stmt =
		conn.prepareStatement("SELECT relname FROM persistent_instances WHERE relname = '" + name.toLowerCase() + "'");
	    ResultSet rs = stmt.executeQuery();
	    boolean retval = rs.next();
	    rs.close();
	    stmt.close();
	    return retval;
	} catch(Exception e) {
	    e.printStackTrace();
	}
	return false;
    }

    public static void createReplicaIfNotExist(String name, String[] contents, Connection conn) {
	try {
	    if (!checkDB(name,conn)) {
		conn.setAutoCommit(false);
		PreparedStatement createTable, hyperTable, insertInstance, createTrigger;
		StringJoiner sj = new StringJoiner(",");
		Arrays.stream(contents).forEach(i -> sj.add(String.valueOf(i)));
		createTable = conn.prepareStatement("CREATE TABLE " + name +
						    " (id SERIAL, time TIMESTAMPTZ NOT NULL, " + sj.toString() + ")");
		hyperTable = conn.prepareStatement("SELECT create_hypertable('" + name + "', 'time')");
		insertInstance = conn.prepareStatement("INSERT into persistent_instances VALUES ('" + name.toLowerCase() + "', 'true')");
		createTable.executeUpdate();
		hyperTable.executeQuery();
		insertInstance.executeUpdate();
		createTable.close();
		hyperTable.close();
		insertInstance.close();
		conn.commit();

		createTrigger = conn.prepareStatement("CREATE TRIGGER " + name.toLowerCase() + "_update AFTER INSERT ON " + name.toLowerCase() + " EXECUTE PROCEDURE notify_trigger('" + name.toLowerCase() + "_channel')");
		createTrigger.execute();
		createTrigger.close();
		conn.commit();
		conn.setAutoCommit(true);
	    } else {
		PreparedStatement update, hasCreated = conn.prepareStatement("SELECT active FROM persistent_instances WHERE relname = '" + name.toLowerCase() + "'");
		ResultSet rs = hasCreated.executeQuery();
		if (rs.next()) {
		    if (rs.getBoolean(1)) {
			//			throw new DoubleInstanceException("duplicated persistent signal instance: " + name);
		    } else {
			update = conn.prepareStatement("UPDATE persistent_instances SET active = 'true' WHERE relname = '" + name.toLowerCase() + "'");
			update.executeUpdate();
			update.close();
		    }
		}
		hasCreated.close();
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public static void createDBIfNotExist(String name, String[] contents) 
        throws DoubleInstanceException {
	try {
	    Class.forName("org.postgresql.Driver");
	    String url = null;
	    String user = null;
	    String password = null;
	    if (DBConfig.distributed != null && DBConfig.distributed.equals("true")) {
		Connection nmrslv = DriverManager.getConnection(DBConfig.url,
								DBConfig.user,
								DBConfig.password);
		PreparedStatement stmt = nmrslv.prepareStatement("SELECT url,userid,passwd FROM kvs WHERE id = ?");
		stmt.setString(1, name.toLowerCase());
		ResultSet rs = stmt.executeQuery();
		if (rs.next()) {
		    url = rs.getString("url");
		    user = rs.getString("userid");
		    password = rs.getString("passwd");
		}
		url = url.replaceAll("postgres", "jdbc:postgresql");
		rs.close();
		stmt.close();
		nmrslv.close();
	    } else if (DBConfig.replicated != null && DBConfig.replicated.equals("true")) {
		url = "jdbc:postgresql://localhost:5432/persistentsignals";
		user = "kamina";
		password = "";
	    } else {
		url = DBConfig.url;
		user = DBConfig.user;
		password = DBConfig.password;
	    }

	    Connection conn = DriverManager.getConnection(url, user, password);
	    if (!checkDB(name,conn)) {
		conn.setAutoCommit(false);
		PreparedStatement createTable, hyperTable, insertInstance;
		StringJoiner sj = new StringJoiner(",");
		Arrays.stream(contents).forEach(i -> sj.add(String.valueOf(i)));
		createTable = conn.prepareStatement("CREATE TABLE " + name +
						    " (id SERIAL, time TIMESTAMPTZ NOT NULL, " + sj.toString() + ")");
		hyperTable = conn.prepareStatement("SELECT create_hypertable('" + name + "', 'time')");
		insertInstance = conn.prepareStatement("INSERT into persistent_instances VALUES ('" + name.toLowerCase() + "', 'true')");
		createTable.executeUpdate();
		hyperTable.executeQuery();
		insertInstance.executeUpdate();
		createTable.close();
		hyperTable.close();
		insertInstance.close();
		conn.commit();
	    } else {
		PreparedStatement update, hasCreated = conn.prepareStatement("SELECT active FROM persistent_instances WHERE relname = '" + name.toLowerCase() + "'");
		ResultSet rs = hasCreated.executeQuery();
		if (rs.next()) {
		    if (rs.getBoolean(1)) {
			//			throw new DoubleInstanceException("duplicated persistent signal instance: " + name);
		    } else {
			update = conn.prepareStatement("UPDATE persistent_instances SET active = 'true' WHERE relname = '" + name.toLowerCase() + "'");
			update.executeUpdate();
			update.close();
		    }
		}
		hasCreated.close();
	    }
	    conn.close();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public static void putInstance(String id, SignalClassInstance signalClassInstance) {
	SignalClassInstance sci = instances.get(id);
	if (sci != null) {
	    instances.remove(id);
	    //sci.discard();
	}
	instances.put(id, signalClassInstance);

	String className = signalClassInstance.getClass().getName();
	SetSignal set = allInstanceMap.get(className);
	if (set != null) set.add(signalClassInstance);
    }

    public static void setAllInstanceMap(String className, SetSignal set) {
	allInstanceMap.put(className, set);
    }
}
