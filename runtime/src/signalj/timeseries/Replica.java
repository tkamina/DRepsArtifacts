/* Copyright (c) 2026, Tetsuo Kamina
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

import java.util.StringJoiner;
import java.util.HashMap;
import java.sql.DriverManager;
import java.time.Duration;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.Notification;
import io.r2dbc.spi.Connection;
import reactor.core.publisher.Mono;

public class Replica extends Synchronizer {
    private String url = "jdbc:postgresql://localhost:5432/persistentsignals";
    private String user = "kamina";
    private String passwd = "";
    private Mono<PostgresqlConnection> receiver = null;
    private Connection pgconn;
    //    private ConnectionPool pool = null;
    //    private Disposable receiverSubscription = null;
    private SignalClassInstance sci;

    public Replica(String dbName, SignalClassInstance inst, String url, String user, String passwd) {
	this(dbName, inst);
	if (url != null) {
	    this.url = url;
	    this.user = user;
	    this.passwd = passwd;
	}
    }
    
    public Replica(String dbName, SignalClassInstance inst) {
	super(dbName, inst);
	this.sci = inst;
    }

    public void delay(PersistentSignal ps, String value) {
	if (map.get(ps) != null) return;

	map.put(ps, value);
	if (punion || --delay == 0) {
	    //	    System.out.println("REPLICA " + dbName + "." + ps.name() + " UPDATE DB");
	    StringBuffer stmt = new StringBuffer();
	    stmt.append("INSERT INTO " + dbName + "(time, ");
	    StringJoiner columns = new StringJoiner(",");
	    StringJoiner values = new StringJoiner(",");
	    for (PersistentSignal key : map.keySet()) {
		columns.add(key.name());
		values.add(map.get(key));
	    }
	    stmt.append(columns.toString());
	    String ts = timeCursor == null ? "NOW()" : "'" + timeCursor + "'";
	    stmt.append(") VALUES (" + ts + ", ");
	    stmt.append(values.toString());
	    stmt.append(")");
	    exec(stmt.toString());
	    for (PersistentSignal key : map.keySet()) {
		map.put(key, null);
	    }
	    delay = size;
	} else {
	    System.out.println("REPLICA " + ps.name() + " DELAY");
	}
    }

    public void initReplica(HashMap<PersistentSignal, String> map) {
	for (PersistentSignal ps : map.keySet()) {
	    this.map.put(ps, null);
	}
	size = map.size();
	delay = map.size();
	try {
	    Class.forName("org.postgresql.Driver");
	    conn = DriverManager.getConnection(url, user, passwd);
	} catch (Exception e) {
	    System.err.println("cannot connect to the replica");
	    return;
	}

	int numColumns = map.keySet().size();
	String[] columns = new String[numColumns];
	int i = 0;
	for (PersistentSignal ps : map.keySet()) {
	    ps.setConnection(conn);
	    // TODO: the type of column must be resolved dynamically
	    columns[i++] = ps.name() + " TEXT";
	}
	Timeseries.createReplicaIfNotExist(dbName, columns, conn);
	setListener();
	setLockListener(url, user, passwd);
    }

    private void setListener() {
	String address = url.split("://")[1];
	String host = address.split(":")[0];
	String port = address.split(":")[1].split("/")[0];
	String database = address.split(":")[1].split("/")[1].split("\\?")[0];
	PostgresqlConnectionFactory connFactory = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
										  .host(host)
										  .port(Integer.parseInt(port))
										  .username(user)
										  .password(passwd)
										  .database(database)
										  .enableSsl()
										  .build());

	receiver = connFactory.create();

	String channel = dbName + "_channel";
	System.out.println("LISTEN channel: " + channel);
	receiver.flatMapMany(pgconn -> {
		this.pgconn = pgconn;
		return pgconn.createStatement("LISTEN " + channel)
		    .execute()
		    .flatMap(result -> result.getRowsUpdated())
		    //		    .then(Mono.fromRunnable(() ->
		    //					    System.out.println("LISTEN command completed: " + channel)))
		    .thenMany(pgconn.getNotifications())
		    .doOnSubscribe(s -> {
			    //			    System.out.println("Subscribed to notifications");
			    sci.getSynchronizer().onReplicaInitCompletion();})
		    .doOnNext(notification -> {
			    //			    System.out.println("Received notification: " + notification.getName());
			    notifyUpdate();
			})
		    .doOnError(error -> {
			    System.err.println("Notification stream failed");
			    error.printStackTrace();
			});})
	    .subscribe(notification -> {},
		       error -> {
			   System.err.println("R2DBC LISTEN error:");
			   error.printStackTrace();
		       });
    }

    private void stopListener() {
	if (pgconn != null) {
	    Mono.from(pgconn.createStatement("UNLISTEN " + dbName + "_channel").execute())
		.then(Mono.from(pgconn.close()))
		.subscribe(null,
			   error -> System.out.println(error.getMessage()),
			   () -> System.out.println("Stopping LISTEN")
			   );
	}
    }

    public void closeConnection() {
	stopListener();
	super.closeConnection();
    }

    private void notifyUpdate() {
	//	System.out.println("Notify from replica: " + System.identityHashCode(this));
	thisSignalClassInstance.getSynchronizer().decrementWaitCount();
    }

    public void setLockListener(String url, String user, String passwd) {
	String address = url.split("://")[1];
	String host = address.split(":")[0];
	String port = address.split(":")[1].split("/")[0];
	String database = address.split(":")[1].split("/")[1].split("\\?")[0];
	PostgresqlConnectionFactory connFactory = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
										  .host(host)
										  .port(Integer.parseInt(port))
										  .username(user)
										  .password(passwd)
										  .database(database)
										  .enableSsl()
										  .build());

	lockReceiver = connFactory.create();

	System.out.println("LISTEN channel: persistent_instances_channel");
	lockReceiver.flatMapMany(pgconn -> {
		return pgconn.createStatement("LISTEN persistent_instances_channel")
		    .execute()
		    .flatMap(result -> result.getRowsUpdated())
		    //		    .then(Mono.fromRunnable(() ->
		    //					    System.out.println("LISTEN command completed: " + channel)))
		    .thenMany(pgconn.getNotifications())
		    .doOnNext(notification -> {
			    //			    System.out.println("Received notification: " + notification.getName());
			    lockLatchCountdown();
			})
		    .doOnError(error -> {
			    System.err.println("Notification stream failed");
			    error.printStackTrace();
			});})
	    .subscribe(notification -> {},
		       error -> {
			   System.err.println("R2DBC LISTEN error:");
			   error.printStackTrace();
		       });
    }
}
