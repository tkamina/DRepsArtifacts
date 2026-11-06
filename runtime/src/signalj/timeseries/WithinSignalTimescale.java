/* Copyright (c) 2019-2022, Tetsuo Kamina
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
import java.sql.Timestamp;
import java.sql.SQLException;

public class WithinSignalTimescale<T> extends TimescaleViewSignal<T> {

    String columns, interval;

    WithinSignalTimescale(Timestamp ts, String interval, String baseName, String name, String fromName, String columnName) {
	super(baseName, name, fromName, columnName);
	columns = "id,time," + (columnName==null ? "value" : columnName + " AS value");
	this.interval = interval;
	preparedQuery = (ts == Timeseries.now ?
			 "SELECT " + columns + " FROM " + fromDB +
			 " WHERE time > NOW() - interval '" + interval + "'" :
			 "SELECT " + columns + " FROM " + fromDB +
			 " WHERE time > TIMESTAMP '" + ts + "' - interval '" + interval + "'");
	//	connectDB(dbName);
    }

    public void snapshot(Timestamp ts) {
	super.snapshot(ts);
	PreparedStatement stmt;
	try {
	    String sql = "CREATE OR REPLACE VIEW " + dbName + " AS SELECT " + columns + " FROM " + fromDB + " WHERE time > TIMESTAMP '" + ts + "' - interval '" + interval + "'";
	    conn.setAutoCommit(false);
	    stmt = conn.prepareStatement(sql);
	    stmt.executeUpdate();
	    stmt.close();
	    conn.commit();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
    }

}
