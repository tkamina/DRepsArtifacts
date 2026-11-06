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

import java.io.File;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.sql.*;
import java.lang.annotation.*;
import signalj.timeseries.annotation.*;
import signalj.timeseries.consist.ConsistLevel;
import signalj.timeseries.consist.ConsistLevelResolver;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.Notification;
//import io.r2dbc.spi.Connection;
import reactor.core.publisher.Mono;

public class Synchronizer {
    protected String dbName;
    protected Connection conn;
    protected int size = 0, delay = 0;
    protected SignalClassInstance thisSignalClassInstance;
    protected HashMap<PersistentSignal, String> map;
    private Vector<SignalClassInstance> externList; // upstreams;
    private Vector<Synchronizer> originalSources; // source signals;
    private Vector<Synchronizer> downstreams;
    private HashMap<SignalClassInstance, Boolean> bufferlessMap;
    private HashMap<String, Queue<Timestamp>> bufferedMap;
    protected Timestamp timeCursor = null, lastCheckPoint = null;
    protected boolean union = true, bufferless = true, punion = false;
    private int interval = -1, checkpointProceedCount = -1;
    private Vector<Synchronizer> sourceSyncs;
    private Vector<Thread> sourceThreads;
    //    private Synchronizer checkpointSynchronizer = null;
    private Map<Timestamp, Integer> waitListMap = new HashMap<Timestamp, Integer>();
    private List<Synchronizer> listeners = new ArrayList<Synchronizer>();

    public boolean listenFlag = false, listenCptFlag = false;
    private Thread checkpointTimer = null;

    private String replicaUrl = null;
    private String replicaUser = null;
    private String replicaPassword = null;

    private Vector<SignalClassInstance> interactionExternList = new Vector<SignalClassInstance>();
    private Vector<SignalClassInstance> interactionDownstreams = new Vector<SignalClassInstance>();

    ConsistLevel cLevel;
    private Vector<Synchronizer> replicas = new Vector<Synchronizer>();
    //    private boolean lock = false;
    private CountDownLatch waitCount = new CountDownLatch(0);
    private CountDownLatch lock = new CountDownLatch(0);
    private CountDownLatch replicaInit = new CountDownLatch(0);

    private boolean lastAmongTheDepth = false;
    private boolean lastAmongTheDepthInteraction = false;

    private boolean fresh = true;

    protected Mono<PostgresqlConnection> lockReceiver = null;

    /*
     * Algorithm 4:
     * This CountDownLatch is used for synchronizing all replica's updates
     * when the consistency level is SER.
     */
    public CountDownLatch getWaitCount() {
	return waitCount;
    }
    
    public void decrementWaitCount() {
	//	System.out.println("Countdown: " + System.identityHashCode(this));
	waitCount.countDown();
    }

    public boolean isSerializable() {
	return cLevel == ConsistLevel.serializable;
    }

    /*
     * Algorithm 1:
     * When the transaction is finished, unlock all upstream data sources
     * implicitly connected using the interaction mechanism.
     */
    private void unlockInteractionUpstreams() {
	startUnlock();
	for (SignalClassInstance sci : interactionExternList) {
	    Synchronizer s = sci.getSynchronizer();
	    s.startUnlock();
	    s.unlockInteractionUpstreams();
	}
    }

    /*
     * Algorithm 1:
     * When the transaction is finished, unlock all upstream data sources
     * declaratively connected using the signal mechanism.
     */
    public void unlockUpstreams() {
	startUnlock();
	for (SignalClassInstance sci : externList) {
	    Synchronizer s = sci.getSynchronizer();
	    s.startUnlock();
	    s.unlockUpstreams();
	}
    }

    public void setLastAmongTheDepth() {
	lastAmongTheDepth = true;
    }

    private void setLastAmongTheDepthInteraction() {
	lastAmongTheDepthInteraction = true;
    }

    /*
     * Algorithm 2:
     * Locking all replicas if the consistency level is serializable.
     */
    public void startLock() {
	if (cLevel == ConsistLevel.serializable) {
	    lock();
	    for (Synchronizer r : replicas) {
		r.lock();
	    }
	}
    }

    /*
     * Algorithm 3:
     * Unlocking all replicas if the consistency level is serializable.
     */
    public void startUnlock() {
	if (cLevel == ConsistLevel.serializable) {
	    unlock();
	    for (Synchronizer r : replicas) {
		r.unlock();
	    }
	}
    }

    protected void lockLatchCountdown() {
	lock.countDown();
    }

    /*
     * Lock is recorded for each database instance.
     */
    public boolean isLocked() {
	try {
	    PreparedStatement stmt = conn.prepareStatement("SELECT transaction FROM persistent_instances WHERE relname = '" + dbName.toLowerCase() + "'");
	    ResultSet rs = stmt.executeQuery();
	    if (rs.next()) {
		String code = rs.getString(1);
		String thisCode = new Integer(System.identityHashCode(this)).toString();
		//		System.out.println("Locked by " + code);
		if (code == null) return false;
		if (code.equals("")) return false;
		if (!code.equals(thisCode)) return true;
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}
	return false;
    }

    /*
     * Algorithm 2: Locking each replica.
     */
    private void lock() {
	try {
	    if (isLocked()) {
		lock = new CountDownLatch(1);
		lock.await();
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}
	try {
	    PreparedStatement stmt = conn.prepareStatement("UPDATE persistent_instances SET transaction='" + System.identityHashCode(this) + "' WHERE relname = '" + dbName.toLowerCase() + "'");
	    stmt.executeUpdate();
	    stmt.close();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    /*
     * Algorithm 3: Unlocking each replica.
     * Note that this unlocking calls the lockLatchCountdown() method above.
     * See also setLockListener below.
     */
    private void unlock() {
	try {
	    PreparedStatement stmt = conn.prepareStatement("UPDATE persistent_instances SET transaction='' WHERE relname = '" + dbName.toLowerCase() + "'");
	    stmt.executeUpdate();
	    stmt.close();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public Synchronizer(String dbName, SignalClassInstance inst) {
	this.dbName = dbName;
	thisSignalClassInstance = inst;
	Class<?> clazz = thisSignalClassInstance.getClass();
	Annotation[] anns = clazz.getAnnotations();
	bufferlessMap = new HashMap<SignalClassInstance, Boolean>();
	bufferedMap = new HashMap<String, Queue<Timestamp>>();
	
	for (Annotation a : anns) {
	    if (a instanceof bufferless) {
		bufferless = true;
	    }
	    if (a instanceof buffered) {
		bufferless = false;
	    }
	    if (a instanceof mode) {
		if (((mode)a).value().equals("intersection")) {
		    union = true;
		} else {
		    union = false;
		}
	    }
	    if (a instanceof pmode) {
		if (((pmode)a).value().equals("union")) {
		    punion = true;
		}
	    }
	    if (a instanceof checkpointInterval) {
		int duration = ((checkpointInterval)a).value();
		//		System.out.println("getting @checkpointInterval: " + duration);
		if (duration < 0) continue;
		interval = duration;
	    }
	}
	map = new HashMap<PersistentSignal, String>();
	externList = new Vector<SignalClassInstance>();
	originalSources = new Vector<Synchronizer>();
	sourceSyncs = new Vector<Synchronizer>();
	sourceThreads = new Vector<Thread>();

	cLevel = ConsistLevelResolver.resolve(clazz);
    }

    public void setConnection(Connection conn) {
	this.conn = conn;
    }

    /*
     * Algorithms 2 and 3:
     * Listen to the update of lock status.
     */
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
										  //  TODO: This switch should be controlled using the configuration.
										  //  .enableSsl()
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

    public void closeConnection() {
	if (conn != null) {
	    try {
		conn.close();
		for (Synchronizer r : replicas) {
		    r.closeConnection();
		}
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	}
    }

    public void setInteractionDownstreams(Vector<SignalClassInstance> ids) {
	interactionDownstreams = ids;
    }

    public void setDownstreams(Vector<Synchronizer> downstreams) {
	this.downstreams = downstreams;
    }

    public void snapshot(Timestamp ts, boolean externFlag) {
	timeCursor = ts;
	if (externFlag) {
	    for (PersistentSignal ps : map.keySet()) {
		ps.snapshot(ts);
	    }
	}
    }

    public void add(PersistentSignal ps) {
	map.put(ps,null);
	size++;
	delay++;
    }

    class CheckPointTimer implements Runnable {
	private int interval;

	public CheckPointTimer(int interval) {
	    this.interval = interval;
	}
	
	public void run() {
	    while (true) {
		try {
		    Thread.sleep(interval);
		    System.out.println("recovery initiated by " + dbName);
		    pushCheckpointing();
		} catch (Exception e) {
		    e.printStackTrace();
		}
	    }
	}

	// starts the checkpointing. executed by sources.
	private void pushCheckpointing() {
	    try {
		PreparedStatement query = conn.prepareStatement("SELECT time FROM persistent_instances WHERE relname = ?");
		query.setString(1, dbName.toLowerCase());
		ResultSet rs = query.executeQuery();
		if (rs.next()) {
		    lastCheckPoint = rs.getTimestamp("time");
		}
		query.close();
		rs.close();
		
		PreparedStatement stmt = conn.prepareStatement("UPDATE persistent_instances SET time = now() WHERE relname = ?");
		stmt.setString(1, dbName.toLowerCase());
		stmt.execute();
		stmt.close();
		//		System.out.println("UPDATE on " + dbName);

		PreparedStatement stmt2 = conn.prepareStatement("DO $$BEGIN CREATE TRIGGER " + dbName.toLowerCase() + "_checkpointTr AFTER UPDATE OF time ON persistent_instances FOR EACH ROW WHEN (NEW.relname = '" + dbName.toLowerCase() + "') EXECUTE PROCEDURE notify_trigger('" + dbName.toLowerCase() + "_checkpoint'); EXCEPTION WHEN duplicate_object THEN null; END;$$");
		stmt2.execute();
		stmt2.close();
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	}
    }

    public Timestamp getLastCheckpoint() { return lastCheckPoint; }

    private void startTimer(int interval) {
	if (checkpointTimer == null) {
	    checkpointTimer = new Thread(new CheckPointTimer(interval*1000));
	    checkpointTimer.start();
	    sourceThreads.add(checkpointTimer);
	}
    }

    public boolean isSourceSync() {
	return sourceSyncs.size() == 0;
    }

    public synchronized void pushCheckPoint(Timestamp lastCheckPoint, Timestamp currentCheckPoint) {
	System.out.println("externList.size() : " + externList.size());
	if (checkpointProceedCount < 0) checkpointProceedCount = externList.size() - 1;
	if (checkpointProceedCount == 0) {
//	System.out.println("start checkpoint procedure on " + dbName);
	    thisSignalClassInstance.ensureConsistency(lastCheckPoint, currentCheckPoint);
	}
	checkpointProceedCount--;
    }

    public void addExtern(SignalClassInstance sci) {
	if (sci == null) return;
	
	if (!externList.contains(sci)) {
	    externList.add(sci);
	    sourceSyncs = getSourceSyncs(sci, new Vector<Synchronizer>());
	    if (interval > 0) {
		// all sources start the checkpoint timer
		for (Synchronizer s : sourceSyncs) {
		    //		    if (checkpointSynchronizer == null) {
		    //			checkpointSynchronizer = s;
		    s.startTimer(interval);
			//		    }
		    //		    break;
		}
	    }
	    if (union) {
		if (bufferless) {
		    bufferlessMap.put(sci,false);
		} else {
		    bufferedMap.put(sci.getID(), new LinkedList<Timestamp>());
		}
	    }
	    sci.addExternSync(this);
	}
    }

    private Vector<Synchronizer> getSourceSyncs(SignalClassInstance sci, Vector<Synchronizer> temp) {
	if (sci == null) return temp;
	
	Vector<SignalClassInstance> upstreams = sci.getSynchronizer().externList;
	if (upstreams.size() == 0) {
	    temp.add(sci.getSynchronizer());
	    return temp;
	}
	for (SignalClassInstance inst : upstreams) {
	    temp = getSourceSyncs(inst,temp);
	}
	return temp;
    }

    public Vector<Timestamp> getCheckPointTimestamps(Timestamp lastCheckPoint, Timestamp currentCheckPoint) {
	Vector<Timestamp> status = new Vector<Timestamp>();
	try {
	    PreparedStatement statusQuery = conn.prepareStatement("SELECT time FROM " + dbName.toLowerCase() + " WHERE time >= ? AND time <= ?");
	    statusQuery.setTimestamp(1, lastCheckPoint);
	    statusQuery.setTimestamp(2, currentCheckPoint);
	    ResultSet statusRs = statusQuery.executeQuery();
	    while (statusRs.next()) {
		status.add(statusRs.getTimestamp("time"));
	    }
	    //	    System.out.println(status.size() + " rows in " + dbName + " from " + lastCheckPoint + " to " + currentCheckPoint);
	    statusRs.close();
	    statusQuery.close();
	} catch (Exception e) {
	    e.printStackTrace();
	}
	return status;
    }

    public Vector<Vector<Timestamp>> getUpstreamCheckPointTimestamps(Timestamp lastCheckPoint, Timestamp currentCheckPoint) {
	Vector<Vector<Timestamp>> retval = new Vector<Vector<Timestamp>>();
	for (SignalClassInstance sci : externList) {
	    Vector<Timestamp> upStamps = sci.getSynchronizer().getCheckPointTimestamps(lastCheckPoint, currentCheckPoint);
	    retval.add(upStamps);
	}
	return retval;
    }

    public void synchronize() {
	/*
	for (SignalClassInstance sci : externList) {
	    Vector<Timestamp> tsl = sci.timestampList();
	    for (Timestamp ts : tsl) {
		for (PersistentSignal ps : map.keySet()) {
		    if (ts.before(ps.latestTimestamp())) {
			sci.snapshot(ts);
			this.snapshot(ts, true);
			ps.pushSignalClassInst();
		    }
		}
	    }
	}
	*/
    }

    public void reval(SignalClassInstance exInst) {
	//	System.out.println("Synchronizer.reval()");
	//	System.out.println(getSignalClassInst().getID() + " Synchronizer.reval(): union: " + union + " bufferless: " + bufferless);
	if (union) {
	    Timestamp ts = exInst.latestTimestamp();
	    if (bufferless) {
		bufferlessMap.replace(exInst, true);
		/*
		for (boolean b: bufferlessMap.values()) {
		    if (!b) return;
		}
		*/
		for (SignalClassInstance inst : bufferlessMap.keySet()) {
		    if (inst.latestTimestamp().after(ts)) {
			ts = inst.latestTimestamp();
		    }
		}
		thisSignalClassInstance.snapshot(ts);
		snapshot(ts,false);
		for (PersistentSignal ps : map.keySet()) {
		    ps.pushSignalClassInst();
		    break;
		}
		bufferlessMap.replaceAll((k,v)-> false);
	    } else {
		bufferedMap.get(exInst.getID()).offer(exInst.latestTimestamp());
		for (Queue<Timestamp> q: bufferedMap.values()) {
		    if (q.peek() == null) {
			//			System.out.println("waiting...");
			return;
		    }
		}
		for (Map.Entry<String, Queue<Timestamp>> entry: bufferedMap.entrySet()) {
		    Timestamp instTs = entry.getValue().poll();
		    for (SignalClassInstance sci: externList) {
			if (sci.getID().equals(entry.getKey())) {
			    sci.snapshot(instTs);
			}
		    }
		    if (instTs.after(ts)) {
			ts = instTs;
		    }
		}
		for (PersistentSignal ps : map.keySet()) {
		    ps.pushSignalClassInst();
		}
	    }
	} else {
	    for (SignalClassInstance sci : externList) {
		sci.snapshot(new Timestamp(System.currentTimeMillis()));
	    }
	    for (PersistentSignal ps : map.keySet()) {
		ps.pushSignalClassInst();
	    }
	}
    }

    public boolean isCausal() {
	return cLevel == ConsistLevel.causal;
    }

    public void setReplicaInfo(String replicaUrl, String replicaUser, String replicaPassword) {
	this.replicaUrl = replicaUrl;
	this.replicaUser = replicaUser;
	this.replicaPassword = replicaPassword;
    }

    public void initReplicas() {
	if (replicas.size() == 0) {
	    Replica r = new Replica(dbName, thisSignalClassInstance, replicaUrl, replicaUser, replicaPassword);
	    replicaInit = new CountDownLatch(1);
	    r.initReplica(map);
	    try {
		replicaInit.await();
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	    replicas.add(r);
	}
    }

    public void onReplicaInitCompletion() {
	replicaInit.countDown();
    }

    /*
     * Algorithm 4:
     * This method is the central for the writing operation.
     * We modified the original "delay" to implement the algorithm.
     */
    public void delay(PersistentSignal ps, String value) {
	if (map.get(ps) != null) return;

	map.put(ps, value);
	if (punion || --delay == 0) {
	    //	    System.out.println("MASTER " + dbName + "." + ps.name() + " UPDATE DB:" + timeCursor);
	    StringBuffer stmt = new StringBuffer();
	    stmt.append("INSERT INTO " + dbName + "(time, ");
	    StringJoiner columns = new StringJoiner(",");
	    StringJoiner values = new StringJoiner(",");
	    for (PersistentSignal key : map.keySet()) {
		columns.add(key.name());
		values.add(map.get(key));
	    }
	    stmt.append(columns.toString());
	    Timestamp tss = timeCursor != null ? timeCursor : new Timestamp(System.currentTimeMillis());
	    String ts = "'" + tss + "'";
	    //	    String ts = timeCursor == null ? "NOW()" : "'" + timeCursor + "'";
	    stmt.append(") VALUES (" + ts + ", ");
	    stmt.append(values.toString());
	    stmt.append(")");
	    
	    // Algorithm 1: 
	    // Locking downstreams (connected using the interaction mechanism)
	    for (SignalClassInstance sci : interactionDownstreams) {
		sci.getSynchronizer().startLock();
	    }
	    exec(stmt.toString());
	    ArrayList<PersistentSignal> pss = new ArrayList<>(map.keySet());
	    if (pss.size() > 0) {
		pss.get(pss.size()-1).getSignalClassInst().getSynchronizer().setLastAmongTheDepthInteraction();
	    }

	    // Algorithm 4 lines 7-12:
	    // Update all replicas.
	    // If the consistency level is serializable, wait until
	    // all replica's updates are finished.
	    if (cLevel == ConsistLevel.serializable) {
		waitCount = new CountDownLatch(replicas.size());
	    }
	    for (Synchronizer r : replicas) {
		r.delay(ps, value);
	    }
	    if (cLevel == ConsistLevel.serializable) {
		try {
		    //		    System.out.println("Before wait: " + System.identityHashCode(this));
		    waitCount.await();
		} catch (Exception e) {
		    e.printStackTrace();
		}
	    }

	    // Algorithm 1: move on to the next "step <- Depth(i,RN_A)"
	    // (for downstream surrogates connected using the interaction mechanism)
	    for (PersistentSignal key : map.keySet()) {
		if (punion) {
		    if (key.listenInteraction() && key.name().equals(ps.name())) key.effect(tss);
		} else {
		    key.effect(tss);
		}
		map.put(key,null);
	    }
	    
	    // unlock if this is the last node among the transaction
	    if (lastAmongTheDepthInteraction && map.keySet().size() == 0) {
		unlockInteractionUpstreams();
	    }
	    if (lastAmongTheDepth && downstreams.size() == 0) {
		unlockUpstreams();
	    }
	    
	    delay = size;
	} else {
	    // synchronize all replicas
	    for (Synchronizer r : replicas) {
		r.delay(ps, value);
	    }
	}

    }

    protected void exec(String query) {
	PreparedStatement stmt;
	try {
	    stmt = conn.prepareStatement(query);
	    stmt.executeUpdate();
	    stmt.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
    }

    public boolean getUnion() { return union; }

    public Synchronizer[] getSourceSyncs() {
	int size = externList.size();
	Synchronizer[] retval = new Synchronizer[size];
	for (int i=0; i<size; i++) retval[i] = externList.elementAt(i).getSynchronizer();
	return retval;
    }

    public void updateSourceSync(Synchronizer oldSync, Synchronizer newSync) {
	externList.remove(oldSync.getSignalClassInst());
	externList.add(newSync.getSignalClassInst());
    }

    public SignalClassInstance getSignalClassInst() {
	return thisSignalClassInstance;
    }

    public void blockSource() {
	for (Synchronizer s : sourceSyncs) {
	    if (s.isBlocked()) continue;
	    s.getSignalClassInst().block();
	}
    }

    public void unblockSource() {
	for (Synchronizer s : originalSources) {
	    s.getSignalClassInst().unblock();
	}
    }

    public List<Timestamp> queryImprocessingTimestamps(List<Timestamp> lst, Set<Synchronizer> visited) {
	long nanoT = System.nanoTime();
	Timestamp t = getImprocessingTimestamp();

	if (visited.contains(this)) {
	    return lst;
	}
	if (t != null && !lst.contains(t) && !isBlocked()) lst.add(t);
	visited.add(this);
	
	for (SignalClassInstance sci: externList) {
	    sci.getSynchronizer().queryImprocessingTimestamps(lst, visited);
	}

	for (PersistentSignal ps : map.keySet()) {
	    for (Object s : ps.getDownstreams()) {
		((Synchronizer)s).queryImprocessingTimestamps(lst, visited);
	    }
	    break;
	}
	
	return lst;
    }

    private Timestamp getImprocessingTimestamp() {
	for (PersistentSignal ps : map.keySet()) {
	    Timestamp t = ps.getImprocessingTimestamp();
	    if (t != null) return t;
	}
	return null;
    }

    private boolean isBlocked() {
	for (PersistentSignal ps : map.keySet()) {
	    if (ps.isBlocked()) return true;
	}
	return false;
    }

    public List<Synchronizer> queryMostDownstreams(List<Synchronizer> lst, Set<Synchronizer> visited) {
	if(visited.contains(this)) {
	    return lst;
	}
	visited.add(this);
	for (PersistentSignal ps : map.keySet()) {
	    Vector<Synchronizer> down = ps.getDownstreams();
	    if (down.isEmpty()) {
		lst.add(this);
	    }
	    for (Synchronizer s : down) {
		s.queryMostDownstreams(lst, visited);
	    }
	    break;
	}
	for (SignalClassInstance sci : externList) {
	    sci.getSynchronizer().queryMostDownstreams(lst, visited);
	}
	
	return lst;
    }

    public void initWaitListMap() {
	waitListMap = new HashMap<Timestamp, Integer>();
    }

    public void putWaitListMap(Timestamp t, int i) {
	waitListMap.put(t, i);
    }

    public int getWaitListMap(Timestamp t) {
	return waitListMap.get(t);
    }

    public Set<Timestamp> waitListMapKeySet() {
	return waitListMap.keySet();
    }

    public Timestamp latestTimestamp() {
	return thisSignalClassInstance.latestTimestamp();
    }

    public List<Synchronizer> listeners() {
	return listeners;
    }

    public void addListener(Synchronizer s) {
	listeners.add(s);
    }

    public int countWaits(Timestamp t) {
	return 0; // TODO
    }

    public Vector<Synchronizer> getOriginalSources() {
	return originalSources;
    }

    public void mergeSources(Vector<Synchronizer> sources) {
	for (Synchronizer s : sources) {
	    if (!originalSources.contains(s))
		originalSources.add(s);
	}
    }

    public void propagateMerge() {
	for (SignalClassInstance i : externList) {
	    Synchronizer s = i.getSynchronizer();
	    s.mergeSources(originalSources);
	    s.propagateMerge();
	}
    }

    public void updateSource() {
	for (SignalClassInstance i : externList) {
	    Vector<Synchronizer> sources = i.getSynchronizer().getOriginalSources();
	    if (sources.size() == 0) {
		sources.add(i.getSynchronizer());
	    }
	    mergeSources(sources);
	    propagateMerge();
	}
    }

    public void setInteractionUpstream(SignalClassInstance sci) {
	if (!interactionExternList.contains(sci)) {
	    interactionExternList.add(sci);
	}
    }

    public void removeInteractionUpstream(SignalClassInstance sci) {
	if (interactionExternList.contains(sci)) {
	    interactionExternList.remove(sci);
	}
    }
}
