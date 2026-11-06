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

import signalj.Signal;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import io.reactivex.functions.Consumer;
import java.util.Arrays;
import java.util.Vector;
import java.util.Set;
import java.util.List;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import reactor.core.publisher.Mono;

import signalj.timeseries.collection.SetPersistentSignal;
import signalj.timeseries.consist.ConsistLevel;
import signalj.timeseries.consist.ConsistLevelResolver;

public class TimescaleSignal<T> extends Signal<T> implements PersistentSignal<T> {

    private T current;
    protected String name;
    protected String baseName;
    protected String dbName;
    protected Connection conn;
    protected Synchronizer sync;
    private boolean discarded = false;
    protected SignalClassInstance signalClassInst;
    private Vector<Consumer> subscribers = new Vector<Consumer>();
    private Vector<Synchronizer> externSyncs = new Vector<Synchronizer>(); // downstreams
    private Thread listener = null;
    private Mono<PostgresqlConnection> receiver = null, checkPointListener = null;
    private boolean blocked = false;
    private CountDownLatch latch = new CountDownLatch(0);
    //    private CountDownLatch readableLatch = new CountDownLatch(0);

    private String url = null;
    private String user = null;
    private String password = null;
    private Connection nmrslv;
    private boolean hasImprocessingTs = false;
    private boolean punion = false;

    private String replicaUrl = null;
    private String replicaUser = null;
    private String replicaPassword = null;

    private Vector<SignalClassInstance> interactionDownstreams = new Vector<SignalClassInstance>();
    private Vector<SetPersistentSignal> interactionDownstreamSetSignals = new Vector<SetPersistentSignal>();

    // uenojip
    private long nanoTime;

    // intereaction
    private boolean listenInteraction = false;

    protected Timestamp timeCursor = null, lastCheckPoint = null, currentCheckPoint = null;

    private ConsistLevel cLevel;
    protected ArrayList<String> parallelTransaction = new ArrayList<String>();

    public TimescaleSignal() { }

    public PersistentSignal<T> newInstanceInner(String baseName, String name, Synchronizer sync) {
	return new TimescaleSignal<T>(baseName, name, sync);
    }

    protected void connectDB(String id) {
	try {
	    Class.forName("org.postgresql.Driver");
	    if (DBConfig.distributed != null && DBConfig.distributed.equals("true")) {
		nmrslv = DriverManager.getConnection(DBConfig.url,
								DBConfig.user,
								DBConfig.password);
		PreparedStatement stmt = nmrslv.prepareStatement("SELECT url,userid,passwd FROM kvs WHERE id = ?");
		stmt.setString(1, id.toLowerCase());
		ResultSet rs = stmt.executeQuery();
		if (rs.next()) {
		    url = rs.getString("url");
		    user = rs.getString("userid");
		    password = rs.getString("passwd");
		}
		url = url.replaceAll("postgres", "jdbc:postgresql");
		rs.close();
		stmt.close();
		//		nmrslv.close();
	    } else if (DBConfig.replicated != null && DBConfig.replicated.equals("true")) {
		/*
		 * If replication is enabled in the configuration, it uses
		 * the local replica. 
		 * TODO:
		 * This setting should be provided by the directory service
		 * in future.
		 */
		url = "jdbc:postgresql://localhost:5432/persistentsignals";
		user = "kamina";
		password = "";
		replicaUrl = DBConfig.url;
		replicaUser = DBConfig.user;
		replicaPassword = DBConfig.password;
	    } else {
                url = DBConfig.url;
		user = DBConfig.user;
		password = DBConfig.password;
	    }
	    conn = DriverManager.getConnection(url, user, password);

	    PreparedStatement query = conn.prepareStatement("SELECT time FROM persistent_instances WHERE relname = ?");
	    query.setString(1, dbName.toLowerCase());
	    ResultSet checkRs = query.executeQuery();
	    if (checkRs.next()) {
		lastCheckPoint = checkRs.getTimestamp("time");
	    }
	    currentCheckPoint = lastCheckPoint;
	    checkRs.close();
	    query.close();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public TimescaleSignal(String baseName, String name, Synchronizer sync) {
	this.baseName = baseName;
	this.dbName = baseName;
	this.name = name;
	this.sync = sync;
	connectDB(dbName);
	sync.setConnection(this.conn);
	sync.setDownstreams(this.externSyncs);
	if (replicaUrl != null) {
	    sync.setReplicaInfo(replicaUrl, replicaUser, replicaPassword);
	}
	sync.setLockListener(url, user, password);
	//	setConsistencyLevel();
    }

    public void setConnection(Connection conn) {
	this.conn = conn;
    }

    public PersistentSignal<T> setName(String name) {
	this.name = name;
	connectDB(dbName);
	return this;
    }

    public String name() { return name; }

    public String dbName() { return dbName; }

    /*
     * Algorithm 1
     * This method is called on the surrogate where the update propagation
     * is initiated; i.e., its depth is Depth(RN_A).
     * It first aquires locks for all replicas.
     * Actuall DB writing (Algorithm 4) is implemented by Synchronizer.delay().
     *
     * Propagation for downstream surrogates is triggered
     * (1) when it receives an update notification from the DB
     *     (see the propagate() method below).
     * (2) within Synchronizer.delay() (for implicit downstreams connected
     *     using the interaction mechanism.
     */
    public void set(T value) {
	current = value;
	sync.startLock();
	sync.setLastAmongTheDepth();

	if (current instanceof String) {
	    sync.delay(this, "'"+String.valueOf(value)+"'");
	} else {
	    sync.delay(this, String.valueOf(value));
	}
    }

    public Timestamp getImprocessingTimestamp() {
	//	if (!hasImprocessingTs) return null;
	return latestTimestamp();
    }

    public void setUpstream(SignalClassInstance o, SignalClassInstance n) {
	nanoTime = System.nanoTime();
	for (Field f : signalClassInst.getClass().getDeclaredFields()) {
	    try {
		if(f.get(signalClassInst) == o) {
		    sync.blockSource();
		    Set<Synchronizer> visitedNodes = new HashSet<Synchronizer>();
		    Set<Synchronizer> visitedQNodes = new HashSet<Synchronizer>();
		    // improcessing timestamp list
		    List<Timestamp> tl = sync.queryImprocessingTimestamps(new ArrayList<Timestamp>(), visitedNodes);
		    
		    tl.remove(n.latestTimestamp());

		    List<Synchronizer> downMost = sync.queryMostDownstreams(new ArrayList<Synchronizer>(), visitedQNodes);

		    Synchronizer newNode = n.getSynchronizer();
		    newNode.initWaitListMap();

		    for (Timestamp t: tl) {
			newNode.putWaitListMap(t,0);
		    }

		    for (Timestamp t : tl) {
			for (Synchronizer s : downMost) {
			    Timestamp t1 = s.latestTimestamp();
			    if (!s.listeners().contains(newNode)) {
				s.addListener(newNode);
			    }
			    if (t1.before(t)) {
				newNode.putWaitListMap(t, newNode.getWaitListMap(t) + s.countWaits(t));
			    }
			}
		    }
		    for (Timestamp tt : newNode.waitListMapKeySet()) {
			if (newNode.getWaitListMap(tt) > 0) {
			    return;
			}
		    }

		    doSwitch(f, o, n);
		    break;
		}
	    } catch (IllegalAccessException e) {
		e.printStackTrace();
	    }
	}
    }

    private void doSwitch(Field f, SignalClassInstance o, SignalClassInstance n) throws IllegalAccessException {
	f.set(signalClassInst, n);
	Synchronizer oldSync = null;
	for (Synchronizer s : sync.getSourceSyncs()) {
	    if (o != null && s == o.getSynchronizer()) {
		oldSync = s;
		break;
	    }
	}
	n.addDown(sync);
	if (oldSync != null) {
	    sync.updateSourceSync(oldSync, n.getSynchronizer());
	}
	sync.addExtern(n);
	n.getSynchronizer().updateSource();
	sync.updateSource();
	sync.unblockSource();
	//	updateSwitchHistory();
	System.out.println(System.nanoTime() - nanoTime);
    }

    public void updateSwitchHistory() {
	sync.initReplicas();
	
	if (nmrslv != null) {
	    String json = getJSON();
	    try {
		PreparedStatement stmt = nmrslv.prepareStatement("INSERT INTO switch_history(time,id,json) values (now(),'" + signalClassInst.getID() + "','" + json + "')");
		stmt.executeUpdate();
		stmt.close();
		//		System.out.println("History JSON: " + getJSON());
	    } catch (SQLException e) {
		e.printStackTrace();
	    }
	}
    }

    private String getJSON() {
	return "{ \"ID\" : \"" + signalClassInst.getID() + "\", \"upstreams\" : " + Arrays.toString(Arrays.stream(sync.getSourceSyncs()).
												    map(x->x.getSignalClassInst()).
												    map(x->"\""+x.getID()+"\"").
												    toArray())
	    + " }";
    }

    private void setNoSync(T value) { current = value; }

    public T value() { return __signalj__get(); }

    /*
     * Algorithm 5:
     * We note that, for causal data sources, it is always readable,
     * as the SignalJ runtime library uses the blocking JDBC write
     * for the DB updates.
     */
    public T __signalj__get() {
	/*
	if (sync.isCausal()) {
	    Always readable, as the database write uses the blocking JDBC write
	}
	*/
	sync.startLock();
	
	T retval;
	if (timeCursor == null) {
	    retval = current;
	} else {
	    retval = valueWithTimeCursor();
	}

	sync.startUnlock();
	
	return retval;
    }

    protected T valueWithTimeCursor() {
	T retval = null;
	try {
	    PreparedStatement stmt = conn.prepareStatement("SELECT " + name + ", time FROM " + dbName + " WHERE time < '" + timeCursor + "' ORDER BY time DESC LIMIT 1");
	    ResultSet rs = stmt.executeQuery();
	    retval = getValueFromResultSet(rs,1);
	    rs.close();
	    stmt.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	return retval;
    }

    public PersistentSignal<T> within(Timestamp ts, String interval, String name) {
	return TimescaleFactory.<T>newWithin(ts, interval, baseName, name, this.name, this.name);
    }

    public PersistentSignal<T> lastDiff(int offset, String name) {
	return TimescaleFactory.<T>newLastDiff(offset, baseName, name, this.name, this.name);
    }

    public PersistentSignal<Double> distance(PersistentSignal p, String name) {
	return TimescaleFactory.newDistance(p, baseName, name, this.name, this.name);
    }

    public PersistentSignal<Integer> pcount(String name) {
	return TimescaleFactory.<Integer>newAnalytic("count", baseName, name, this.name, this.name);
    }

    public PersistentSignal<Double> avg(String name) {
	return TimescaleFactory.<Double>newAnalytic("avg", baseName, name, this.name, this.name);
    }

    public PersistentSignal<T> first(String name) {
	return TimescaleFactory.<T>newFirst(baseName, name, this.name, this.name);
    }

    public PersistentSignal<T> psum(String name) {
	return TimescaleFactory.<T>newAnalytic("sum", baseName, name, this.name, this.name);
    }

    public PersistentSignal<T> max(String name) {
	return TimescaleFactory.<T>newAnalytic("max", baseName, name, this.name, this.name);
    }

    public PersistentSignal<T> min(String name) {
	return TimescaleFactory.<T>newAnalytic("min", baseName, name, this.name, this.name);
    }

    public void dbgen() { } // throws DoubleInstanceException { }

    protected String tableType() { return "TABLE"; }

    public void discard() {
	try {
	    if (!discarded) {
		conn.setAutoCommit(false);
		PreparedStatement stmt = conn.prepareStatement("DELETE FROM persistent_instances WHERE relname = '" + dbName.toLowerCase() + "'");
		PreparedStatement dropStmt = conn.prepareStatement("DROP " + tableType() + " " + dbName.toLowerCase());
		stmt.executeUpdate();
		stmt.close();
		dropStmt.executeUpdate();
		dropStmt.close();
		conn.commit();
		conn.close();
		discarded = true;
	    }
	} catch (Exception e) {  }
    }

    protected T getValueFromResultSet(ResultSet rs, int column) throws SQLException {
	T retval = null;
	ResultSetMetaData meta = rs.getMetaData();
	int t = meta.getColumnType(1);
	if (rs.next()) {
	    if (t == Types.BOOLEAN) retval = (T)(Boolean)rs.getBoolean(1);
	    else if (t == Types.TINYINT) retval = (T)(Byte)rs.getByte(1);
	    else if (t == Types.SMALLINT) retval = (T)(Short)rs.getShort(1);
	    else if (t == Types.INTEGER) retval = (T)(Integer)rs.getInt(1);
	    else if (t == Types.REAL) retval = (T)(Float)rs.getFloat(1);
	    else if (t == Types.DOUBLE) retval = (T)(Double)rs.getDouble(1);
	    else if (t == Types.VARCHAR) retval = (T)rs.getString(1);
	}
	if (retval == null) {
	    if (t == Types.BOOLEAN) retval = (T)(Boolean)false;
	    else if (t == Types.TINYINT) retval = (T)(Integer)0;
	    else if (t == Types.SMALLINT) retval = (T)(Integer)0;
	    else if (t == Types.INTEGER) retval = (T)(Integer)0;
	    else if (t == Types.REAL) retval = (T)(Double)0.0;
	    else if (t == Types.DOUBLE) retval = (T)(Double)0.0;
	    else if (t == Types.VARCHAR) retval = (T)"";
	}
	return retval;
    }

    public void setSignalClassInst(SignalClassInstance signalClassInst) {
	this.signalClassInst = signalClassInst;
    }

    public SignalClassInstance getSignalClassInst() {
	return signalClassInst;
    }

    public void pushSignalClassInst() {
	//	System.out.println("pushSignalClassInst from " + dbName);
	hasImprocessingTs = true;
	signalClassInst.reval();
    }

    public void resume() {
	try {
	    PreparedStatement stmt = conn.prepareStatement("SELECT " + name + " FROM " + dbName + " WHERE " + name + " IS NOT NULL ORDER BY time DESC");
	    ResultSet rs = stmt.executeQuery();
	    setNoSync(getValueFromResultSet(rs,1));
	    rs.close();
	    stmt.close();
	} catch (Exception e) { e.printStackTrace(); }
    }

    public void reset() {
	try {
	    PreparedStatement stmt = conn.prepareStatement("DELETE from " + dbName);
	    stmt.executeUpdate();
	    stmt.close();
	} catch (Exception e) { e.printStackTrace(); }
    }

    public boolean isInitialized() {
	try {
	    PreparedStatement stmt = conn.prepareStatement("SELECT " + name + " FROM " + dbName);
	    ResultSet rs = stmt.executeQuery();
	    return rs.next();
	} catch (Exception e) { e.printStackTrace(); }
	return false;
    }

    public void setIfNotInitialized(T value) {
	if (!isInitialized()) set(value);
    }

    public void snapshot(Timestamp ts) {
	//	System.out.println("snapshot on " + dbName + " value " + ts);
	timeCursor = ts;
	if (sync != null) sync.snapshot(ts, false);
    }

    public Timestamp latestTimestamp() {
	return timestampQuery(" DESC");
    }

    public Timestamp firstTimestamp() {
	return timestampQuery("");
    }

    private Timestamp timestampQuery(String desc) {
	Timestamp ts = null;
	try {
	    PreparedStatement stmt = conn.prepareStatement("SELECT time FROM " + dbName + " ORDER BY time" + desc);
	    ResultSet rs = stmt.executeQuery();
	    if (rs.next()) {
		ts = rs.getTimestamp("time");
	    }
	    rs.close();
	    stmt.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	if (ts == null) ts = new Timestamp(System.currentTimeMillis());
	return ts;
    }

    public <S extends T> void psubscribe(Object c) {
	subscribers.add((Consumer<S>)c);
    }

    public void effect(Timestamp ts) {
	for (SignalClassInstance sci : interactionDownstreams) {
	    sci.snapshot(ts);
	}
	//	System.out.println(dbName + " current in effect:" + current);
	for (Consumer c : subscribers) {
	    try {
		c.accept(current);
		//		c.accept(__signalj__get());
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	}
    }

    public Vector<Timestamp> timestampList() {
	Vector<Timestamp> retval = new Vector<Timestamp>();
	try {
	    PreparedStatement stmt = conn.prepareStatement("SELECT time FROM " + dbName + " ORDER BY time DESC");
	    ResultSet rs = stmt.executeQuery();
	    while (rs.next()) {
		retval.add(rs.getTimestamp("time"));
	    }
	    rs.close();
	    stmt.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	return retval;
    }

    private void setSeveralListeners() {
	String address = url.split("://")[1];
	String host = address.split(":")[0];
	String port = address.split(":")[1].split("/")[0];
	String database = address.split(":")[1].split("/")[1].split("\\?")[0];
	PostgresqlConnectionFactory connFactory = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
		    .host(host)
										  .port(Integer.parseInt(port))
										  .username(user)
										  .password(password)
										  .database(database)
										  //.enableSsl()
										  .build());
	receiver = connFactory.create();
	startListen(dbName);
	checkPointListener = connFactory.create();
	startListenCheckpoint(dbName);
    }
    
    public void addExternSync(Synchronizer sync) {
	if (!externSyncs.contains(sync)) externSyncs.add(sync);
	if (receiver == null) {
	    setSeveralListeners();
	}
    }

    private void startListenCheckpoint(String tableName) {
	if (!sync.listenCptFlag) {
	    checkPointListener.map(pgconn -> {
		    return pgconn.createStatement("Listen " + tableName + "_checkpoint")
			.execute()
			.thenMany(pgconn.getNotifications())
			.doOnNext(notification -> checkPoint())
			.subscribe();
		}).subscribe();
	    sync.listenCptFlag = true;
	}
    }

    private void startListen(String tableName) {
	if (!sync.listenFlag) {
	    receiver.map(pgconn -> {
		    return pgconn.createStatement("LISTEN " + tableName + "_channel")
			.execute()
			//		    .flatMap(PostgresqlResult::getRowsUpdated)
			.thenMany(pgconn.getNotifications())
			.doOnNext(notification -> propagate())
			.subscribe();
		}).subscribe();
	    sync.listenFlag = true;
	}
    }

    /*
     * Algorithm 1: Move on to the next "step <- Depth(i,RN_A)"
     */
    private void propagate() {
	//	System.out.println(dbName + "'s start time," + System.nanoTime());

	// If seriarizable, waiting for all replica's updates (Algorithm 4).
	if (sync.isSerializable()) {
	    try {
		sync.getWaitCount().await();
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	}
	if (blocked) {
	    try {
		latch.await();
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	}
	// lock downstreams
	for (Synchronizer sync : externSyncs) {
	    sync.startLock();
	}
	if (externSyncs.size() > 0) {
	    externSyncs.elementAt(externSyncs.size()-1).setLastAmongTheDepth();
	}
	// DB writing is implemented in the Synchronizer class.
	for (Synchronizer sync : externSyncs) {
	    Timestamp ts = timeCursor != null ? timeCursor : new Timestamp(System.currentTimeMillis());
	    signalClassInst.snapshot(ts);
	    sync.snapshot(ts,true);
	    sync.reval(signalClassInst);
	}
	hasImprocessingTs = false;
	//	System.out.println(dbName + "'s end time," + System.nanoTime());
    }

    public void block() {
	blocked = true;
	latch = new CountDownLatch(1);
    }
    public void unblock() {
	blocked = false;
	latch.countDown();
    }
    public boolean isBlocked() { return blocked; }

    private void checkPoint() {
	if (sync.isSourceSync()) {
	    updateCheckPoint();
	    //	    System.out.println(dbName + " is source, just skipped");
	}
	// propagating downstream instances.
	propagateRecovery(lastCheckPoint, currentCheckPoint);
	//	for (Synchronizer sync : externSyncs) {
	//	    sync.pushCheckPoint(lastCheckPoint, currentCheckPoint);
	//	}
    }

    private void propagateRecovery(Timestamp lastCheckPoint, Timestamp currentCheckPoint) {
	for (Synchronizer sync : externSyncs) {
	    sync.pushCheckPoint(lastCheckPoint, currentCheckPoint);
	}
    }

    private void updateCheckPoint() {
	lastCheckPoint = currentCheckPoint;
	try {
	    PreparedStatement query = conn.prepareStatement("SELECT time FROM persistent_instances WHERE relname = ?");
	    query.setString(1, dbName.toLowerCase());
	    ResultSet rs = query.executeQuery();
	    if (rs.next()) {
		currentCheckPoint = rs.getTimestamp("time");
	    }
	    rs.close();
	    query.close();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public void ensureConsistency(Timestamp lastCheckPoint, Timestamp currentCheckPoint) {
	System.out.println("ensuring consistency of " + dbName + " from " + lastCheckPoint + " to " + currentCheckPoint);
	Vector<Timestamp> status = sync.getCheckPointTimestamps(lastCheckPoint, currentCheckPoint);
	Vector<Vector<Timestamp>> upStamps = sync.getUpstreamCheckPointTimestamps(lastCheckPoint, currentCheckPoint);
	compareAndRestore(upStamps, status);
	propagateRecovery(lastCheckPoint, currentCheckPoint);
    }

    private void compareAndRestore(Vector<Vector<Timestamp>> upStamps, Vector<Timestamp> status) {
	if (sync.getUnion()) {
	    Vector<Timestamp> tmp = upStamps.elementAt(0);
	    for (int i=0; i<tmp.size(); i++) {
		boolean restoreRequired = true;
		Timestamp tmax = null;
		for (Vector<Timestamp> upStamp: upStamps) {
		    Timestamp t = upStamp.elementAt(i);
		    if (status.contains(t)) restoreRequired = false;
		    if (tmax == null) tmax = t;
		    else if (tmax.after(t)) tmax = t;
		}
		if (restoreRequired) restore(tmax);
	    }
	} else {
	    for (Vector<Timestamp> upStamp: upStamps) {
		for (Timestamp t : upStamp) {
		    if (!status.contains(t)) restore(t);
		}
	    }
	}
    }

    /*
    private void compareAndRestore(Vector<Timestamp> upStamp, Vector<Timestamp> status) {
	int upIndex = 0, thisIndex = 0;
	while (upIndex < upStamp.size()) {
	    Timestamp up = upStamp.elementAt(upIndex);
	    if (thisIndex >= status.size()) {
		restore(up);
		upIndex++;
		continue;
	    }
	    Timestamp me = status.elementAt(thisIndex);
	    if (up.equals(me)) {
		upIndex++;
		thisIndex++;
		continue;
	    }
	    restore(up);
	    upIndex++;
	}
    }
    */

    private void restore(Timestamp ts) {
	Timestamp latest = signalClassInst.latestTimestamp();
	signalClassInst.snapshot(ts);
	signalClassInst.reval();
	signalClassInst.snapshot(latest);
    }

    public void addDown(Synchronizer down) {
	externSyncs.add(down);
    }

    public Vector<Synchronizer> getDownstreams() {
	return externSyncs;
    }

    public boolean listenInteraction() {
	return listenInteraction;
    }

    public void setListenInteraction(boolean v) {
	listenInteraction = v;
    }

    public void setInteractionDownstream(SignalClassInstance sci) {
	interactionDownstreams.add(sci);
	sci.getSynchronizer().setInteractionUpstream(this.signalClassInst);
	if (receiver == null) {
	    setSeveralListeners();
	}
    }

    public void removeInteractionDownstream(SignalClassInstance sci) {
	interactionDownstreams.remove(sci);
	sci.getSynchronizer().closeConnection();
	sci.getSynchronizer().removeInteractionUpstream(this.signalClassInst);
    }

    public void setInteractionDownstream(SetPersistentSignal sps) {
	interactionDownstreamSetSignals.add(sps);
	sps.addInteractionListener(this);
	for (Object sci : sps) {
	    setInteractionDownstream((SignalClassInstance)sci);
	}
    }

    public void updateInteractionDownstream(SignalClassInstance obj, String op) {
	if (op.equals("remove")) {
	    removeInteractionDownstream(obj);
	} else if (op.equals("add")) {
	    setInteractionDownstream(obj);
	}
    }

    /*
    class Listener implements Runnable {
	private PGConnection pgconn;

	Listener(Connection conn, String tableName) {
	    try {
		pgconn = conn.unwrap(PGConnection.class);
		Statement stmt = conn.createStatement();
		stmt.execute("LISTEN " + tableName + "_channel");
		stmt.close();
	    } catch (SQLException e) {
		e.printStackTrace();
	    }
	}

	public void run() {
	    try {
		while(true) {
		    PGNotification notifications[] = pgconn.getNotifications();
		    //		    System.out.println("notification size: " + notifications.length);
		    if (notifications != null) {
			for (Synchronizer sync : externSyncs) {
			    Timestamp ts = new Timestamp(System.currentTimeMillis());
			    signalClassInst.snapshot(ts);
			    sync.snapshot(ts,true);
			    sync.reval(signalClassInst);
			}
		    }
		}
	    } catch (Exception e) { e.printStackTrace(); }
	}
    }
    */
}

