---- MODULE DRepsLocal ----
EXTENDS Naturals, FiniteSets, Sequences, TLC

(*
 * HOW THIS STATE-TRANSITION MODEL CORRESPONDS TO THE PAPER
 *
 * This model is an abstract, finite-state model of the transaction and
 * replication protocol, not a line-by-line operational encoding of the
 * model and algorithms described in Sections 4 and 5.
 * In particular:
 *
 *  Paper concept / algorithm               TLA+ representation
 *  --------------------------              -------------------------------
 *  global DAG G=(P,E)                      Nodes, Edge, Depth
 *  application projection D_A / RN_A       Visible, ReplicasOf, AppRep
 *  physical(r)                             Phys, NodeOf(r)
 *  consistency annotation                  Level[a][n]
 *  Algorithm 1: transaction start          Start(a)
 *  Algorithm 1: ordered replica steps      NeedSeq(a), pc[a], Acquire(a,r)
 *  Algorithm 1: Write for each node        CanWrite(a,n), Write(a,n)
 *  Algorithm 1: final unlock               Commit(a)
 *  Algorithm 2: lock all replicas of SER   NeedSet(a), CanAcquire(a,r), Acquire(a,r)
 *  Algorithm 3: unlock all SER replicas    Commit(a) (owner entries reset to Nil)
 *  Algorithm 4: replicate update           versions and pending in Write(a,n)
 *  Algorithm 4: SER waits for replication  SER write is abstracted as one atomic
 *                                          update of every replica
 *  Algorithm 4: CAUSAL does not wait       pending + independent Deliver steps
 *  Algorithm 5: causal Readable condition  readable, Deliver(r,t), ApplicationLOcalCausal
 *  Algorithm 5: SER read lock              NOT represented as a separate read operation
 *                                          (see below)
 *
 * Important abstraction boundaries:
 * (1) As in the paper, values and the actual read/write computations are abstracted away.
 * (2) The model does not introduce a separate Read action. This is because, in the model,
 *     upstream dependencies are represented by UpstreamWritten(a,n), while causal
 *     visibility is represented by Deliver(r,t)'s readable guard; i.e., the model checks
 *     the resulting timestamp/readability condition, not every invocation of Algorithm 5.
 * (3) SER replication is made atomic in Write(a,n): all replicas receive the timestamp
 *     in one state transition. This abstracts Algorithm 4's asynchronous propagation
 *     followed by waiting until every replica has the timestamp. CAUSAL propagation is
 *     explicitly asynchronous through pending and Deliver(r,t).
 * (4) Locking is represented by owner/held. The model uses one global replica order
 *     (OrderIndex), whose validity is checked against the paper's order: first by
 *     dcreasing dependency depth, then by data-source ID, and within one data source
 *     by replica's ordering.
 * (5) Transactions issued by one application are sequential; different application
 *     and asynchronous deliveries may interleave. This is captured by Start/Commit
 *     and the independent Deliver action.
 *
 * The comments below identify which part of the paper each definition represents and
 * where the model intentionally abstracts the algorithm.
 *)

CONSTANTS (* Definitions are provided for each finite model configuration *)
    Apps, Nodes, Reps, Edge, Depth, NodeId, ReplicaId,
    Phys, ReplicasOf, Visible, Level, AppRep, OrderIndex, Nil,
    MaxTx

SER == "SER"
CAUSAL == "CAUSAL"

VARIABLES
    active,              \* [app -> BOOLEAN]
    txno,                \* [app -> Nat]
    txTs,                \* [app -> Nat]
    pc,                  \* next replica in the global lock order
    held,                \* [app -> SUBSET Reps]
    owner,               \* [replica -> app-specific transaction id or Nil]
    written,             \* [app -> SUBSET Nodes]
    versions,            \* [replica -> SET timestamp]
    readable,            \* [app -> [replica -> SET timestamp]]
    pending,             \* [replica -> SET timestamp]
    history,             \* [app -> Seq transaction-id]
    commitSeq,           \* global sequence of committed transaction ids
    nextTs               \* next globally unique transaction timestamp

vars ==
    <<active, txno, txTs, pc, held, owner, written,
      versions, readable, pending, history, commitSeq, nextTs>>

(* data source of the replica r *)
NodeOf(r) == Phys[r]

(* Section 4, end of the replica-network depth definition:
   Dependency sources (larger depth) precede dependent/sink nodes; ties
   are broken by the data-source ID.  Accompaniying MC_* definitions
   supplie finite Depth/NodeId instances. *)
NodeBefore(n,m) ==
    n # m /\
    (Depth[n] > Depth[m] \/
      (Depth[n] = Depth[m] /\ NodeId[n] < NodeId[m]))
 
(* Section 5.1.1 / Algorithm 2:
   Replicas of one data source are ordered by their ReplicaID (supplied by MC_* ).
   Across data sources, NodeBefore above defines the order.
   Together these form the global lock order used by this finite model. *) 
ReplicaBefore(r,s) ==
    r # s /\
    IF NodeOf(r) = NodeOf(s)
    THEN ReplicaId[r] < ReplicaId[s]
    ELSE NodeBefore(NodeOf(r), NodeOf(s))

Before(r,s) == OrderIndex[r] < OrderIndex[s]

Chosen(a,n) == AppRep[a][n]

(* Sections 4 and 5.1.1 (Algorithm 4).
   Visible[a] is the set of data sources in the application's projection.
   For a SER source, all replicas are required; Algorithm 2 locks the entire
   replica set and Algorithm 4 propagates the update to the entire set.
   For a CAUSAL source, only the application's selected replica participates
   synchronously; other replicas receive asynchronous deliveries. *)
Needed(a,r) ==
    LET n == NodeOf(r) IN
        n \in Visible[a] /\ 
        ((Level[a][n] = SER) \/ r = Chosen(a,n))

NeedSet(a) == { r \in Reps : Needed(a,r) }

(* sequence of replicas in the total order defined above *)
OrderedReps ==
    [i \in 1..Cardinality(Reps) |-> CHOOSE r \in Reps : OrderIndex[r] = i]

(* Replicas to be written in a transaction are ordered according to the total order defined above *)
NeedSeq(a) ==
    SelectSeq(OrderedReps, LAMBDA r : Needed(a,r))

Sources(a,n) ==
    { p \in Visible[a] : <<p,n>> \in Edge }

AllNodesWritten(a) ==
    written[a] = Visible[a]

UpstreamWritten(a,n) ==
    \A p \in Sources(a,n) : p \in written[a]

(* Algorithm 1 lines 3-5 + Algorithm 2:
   a transaction acquires the next replica in the prescribed order.
   owner[r]=Nil models lock(r')=false; for SER, NeedSet contains every
   replica of the data source, so the loop in Algorithm 2 is represented
   by successive Acquire transitions.  CAUSAL replicas are not locked. *)
CanAcquire(a,r) ==
    /\ active[a]
    /\ r \in NeedSet(a)
    /\ owner[r] = Nil
    /\ pc[a] <= Len(NeedSeq(a))
    /\ r = NeedSeq(a)[pc[a]]

Acquire(a,r) ==
    /\ CanAcquire(a,r)
    /\ owner' = [owner EXCEPT ![r] = <<a,txno[a]>>] 
    /\ held' = [held EXCEPT ![a] = @ \cup {r}]
    /\ pc' = [pc EXCEPT ![a] = @ + 1]
    /\ UNCHANGED
         <<active, txno, txTs, written, versions, readable,
           pending, history, commitSeq, nextTs>>

(* Algorithm 1 invokes Write only after the corresponding lock operation.
   UpstreamWritten imposes the paper's source-to-sink update order:
   every direct predecessor of n must already have been written.
   The implication over Needed replicas ensures that all required locks
   for this data source have been acquired before its write. *)
CanWrite(a,n) ==
    /\ active[a]
    /\ n \in Visible[a]
    /\ n \notin written[a]
    /\ UpstreamWritten(a,n)
    /\ \A r \in Reps :
          NodeOf(r) = n /\ Needed(a,r)
          => owner[r] = <<a,txno[a]>> 

(* Abstract counterpart of Algorithm 4, for all replicas of one data source.
   The actual value computation and the optional predecessor Read calls
   (Algorithm 4 lines 1-5) are abstracted.  SER replication is represented
   atomically by adding the timestamp to every replica; this corresponds
   to Algorithm 4 lines 10-12 after its wait has completed.  For CAUSAL,
   the chosen replica is updated immediately and updates to the remaining
   replicas are put in pending, so the transaction need not wait. *)
Write(a,n) ==
    /\ CanWrite(a,n)
    /\ written' = [written EXCEPT ![a] = @ \cup {n}]
    /\ versions' =
          [r \in Reps |->
             IF NodeOf(r) = n /\ Level[a][n] = SER
             THEN versions[r] \cup {txTs[a]}
             ELSE IF r = Chosen(a,n)
                  THEN versions[r] \cup {txTs[a]}
                  ELSE versions[r]]
    /\ pending' = (* replicas asynchronously propagated when n is CAUSAL *)
          [r \in Reps |->
             IF NodeOf(r) = n /\ Level[a][n] = CAUSAL /\ r # Chosen(a,n)
             THEN pending[r] \cup {txTs[a]}
             ELSE pending[r]]
    /\ UNCHANGED
         <<active, txno, txTs, pc, held, owner,
           readable, history, commitSeq, nextTs>>

(* Asynchronous completion of the CAUSAL replication in Algorithm 4.
   The timestamp becomes readable at r only when it is already readable
   at the selected replicas of all direct upstream sources.  This is the
   state-transition counterpart of the causal Readable condition in
   Algorithm 5 / Section 5.1.2. *)
Deliver(r,t) ==
    /\ t \in pending[r]
    /\ versions' =
          [versions EXCEPT ![r] = @ \cup {t}]
    /\ pending' =
          [pending EXCEPT ![r] = @ \ {t}]
    /\ readable' =
          [a \in Apps |->
             [rr \in Reps |->
                IF rr = r /\
                   \A p \in Sources(a,NodeOf(r)) :
                       t \in readable[a][Chosen(a,p)]
                THEN readable[a][rr] \cup {t}
                ELSE readable[a][rr]]]
    /\ UNCHANGED
         <<active, txno, txTs, pc, held, owner, written,
           history, commitSeq, nextTs>>

(* Algorithm 1 line 8 and Algorithm 3: release every lock held by this
   transaction after all data sources have been written.  The model releases
   all its locks in one transition, which is described in Algorithm 3.
   All locks are held by replicas belonging to each SER node.
   
   The transaction history records the sequential per-application commit order (Section 5.2.3). *)
Commit(a) ==
    /\ active[a]
    /\ AllNodesWritten(a)
    /\ held[a] = NeedSet(a)
    /\ active' = [active EXCEPT ![a] = FALSE]
    /\ owner' =
          [r \in Reps |->
             IF owner[r] = <<a,txno[a]>> THEN Nil ELSE owner[r]]
    /\ held' = [held EXCEPT ![a] = {}]
    /\ history' =
          [history EXCEPT ![a] = Append(@, <<a,txno[a]>>)]
    /\ commitSeq' = Append(commitSeq, <<a,txno[a]>>)
    /\ UNCHANGED
         <<txno, txTs, pc, written, versions, readable, pending, nextTs>>

(* Section 5.2.3: each application issues transactions sequentially,
   assigning a fresh timestamp to each transaction.  Different apps may
   start/interleave transactions concurrently. *)
Start(a) ==
    /\ ~active[a]
    /\ txno[a] < MaxTx
    /\ active' = [active EXCEPT ![a] = TRUE]
    /\ txno' = [txno EXCEPT ![a] = @ + 1]
    /\ txTs' = [txTs EXCEPT ![a] = nextTs]
    /\ nextTs' = nextTs + 1
    /\ pc' = [pc EXCEPT ![a] = 1]
    /\ written' = [written EXCEPT ![a] = {}]
    /\ held' = [held EXCEPT ![a] = {}]
    /\ UNCHANGED <<owner, versions, readable, pending, history, commitSeq>>

Next ==
    \/ \E a \in Apps : Start(a)
    \/ \E a \in Apps, r \in Reps : Acquire(a,r)
    \/ \E a \in Apps, n \in Nodes : Write(a,n)
    \/ \E r \in Reps :
       \E t \in pending[r] :
            Deliver(r,t)
    \/ \E a \in Apps : Commit(a)

Init ==
    /\ active = [a \in Apps |-> FALSE]
    /\ txno = [a \in Apps |-> 0]
    /\ txTs = [a \in Apps |-> 0]
    /\ pc = [a \in Apps |-> 1]
    /\ held = [a \in Apps |-> {}]
    /\ owner = [r \in Reps |-> Nil]
    /\ written = [a \in Apps |-> {}]
    /\ versions = [r \in Reps |-> {0}]
    /\ readable = [a \in Apps |-> [r \in Reps |-> {0}]]
    /\ pending = [r \in Reps |-> {}]
    /\ history = [a \in Apps |-> <<>>]
    /\ commitSeq = <<>>
    /\ nextTs = 1

Spec ==
    Init /\ [][Next]_vars

(***************************************************************************)
(* Static/global assumptions                                               *)
(***************************************************************************)

Projection(a) ==
    { <<n,m>> \in Edge : n \in Visible[a] /\ m \in Visible[a] }

GlobalDAG ==
    \A <<n,m>> \in Edge : Depth[n] > Depth[m]

NodeIdUnique ==
    \A n,m \in Nodes : n # m => NodeId[n] # NodeId[m]

ReplicaIdUnique ==
    \A r,s \in Reps :
        r # s /\ NodeOf(r) = NodeOf(s) => ReplicaId[r] # ReplicaId[s]

ProjectionInvariant ==
    \A a \in Apps :
        Projection(a) =
        { <<n,m>> \in Edge : n \in Visible[a] /\ m \in Visible[a] }

LevelGE(c,d) ==
    c = d \/ (c = SER /\ d = CAUSAL)

LevelInvariant ==
    \A a \in Apps :
       \A e \in Projection(a) :
           LevelGE(Level[a][e[1]], Level[a][e[2]])

ChosenInvariant ==
    \A a \in Apps :
       \A n \in Visible[a] :
            Chosen(a,n) \in ReplicasOf[n]

ReplicaCoverage ==
    \A n \in Nodes :
        { r \in Reps : NodeOf(r) = n } = ReplicasOf[n]

(* checking that the replica ordering supplied by MC_* agrees with ReplicaBefore *)
OrderInvariant ==
    /\ \A r \in Reps :
          1 <= OrderIndex[r] /\ OrderIndex[r] <= Cardinality(Reps)
    /\ \A r,s \in Reps :
          r # s => (Before(r,s) \/ Before(s,r))
    /\ \A r,s \in Reps :
          r # s => (Before(r,s) <=> ReplicaBefore(r,s))

WellFormed ==
    /\ GlobalDAG
    /\ NodeIdUnique
    /\ ReplicaIdUnique
    /\ ProjectionInvariant
    /\ LevelInvariant
    /\ ChosenInvariant
    /\ ReplicaCoverage
    /\ OrderInvariant

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)

(*
 * Every held lock belongs to exactly one transaction.
 * This is the key mutual-exclusion invariant.
 *)
MutualExclusion ==
    \A r \in Reps :
        owner[r] = Nil \/
        \E a \in Apps : owner[r] = <<a,txno[a]>> 

(*
 * Global lock-order discipline.
 * If a transaction holds a later required replica, it has already acquired
 * every earlier required replica.
 *)
LockOrderInvariant ==
    \A a \in Apps :
        \A i,j \in 1..Len(NeedSeq(a)) :
            i < j /\ NeedSeq(a)[j] \in held[a]
            => NeedSeq(a)[i] \in held[a]

(*
 * No two transactions can simultaneously own replicas belonging to the
 * same SER node.  Because all SER replicas are in NeedSet(a), this follows
 * from the ordinary mutex plus the acquisition protocol.
 *)
SERReplicaExclusion ==
    \A a,b \in Apps, n \in Nodes :
        a # b /\ n \in Visible[a] /\ n \in Visible[b] /\
        Level[a][n] = SER /\ Level[b][n] = SER
        =>
        ~(\E r,s \in ReplicasOf[n] :
             owner[r] = <<a,txno[a]>> /\
             owner[s] = <<b,txno[b]>>)

(*
 * Application-local SER:
 * each application has a single issuing thread, so its committed
 * transactions have a total history.  In addition, every SER node is
 * exclusively locked for the complete transaction.
 *
 * For this model, serializability is therefore witnessed by history[a].
 *)
ApplicationLocalSER ==
    \A a \in Apps :
        \A i,j \in 1..Len(history[a]) :
            i # j /\ i < j => history[a][i] # history[a][j]

(*
 * Application-local causal consistency:
 * a timestamp is readable at a replica only when it is readable at every
 * directly upstream node replica selected by that application.
 *)
Path(E,n,m,k) ==
    \E p \in [1..k -> Nodes] :
        /\ p[1] = n
        /\ p[k] = m
        /\ \A i \in 1..(k-1) :
              <<p[i], p[i+1]>> \in E

Reachable(E,n,m) ==
    \E k \in 1..Cardinality(Nodes) :
        Path(E,n,m,k)

Ancestors(a,n) ==
    { p \in Visible[a] :
        p # n /\ Reachable(Projection(a),p,n) }

(* Section 5.1.2 / Algorithm 5: application-local causal visibility.
   A timestamp visible at a replica must also be visible at the selected
   replica of every transitive upstream data source.  Deliver enforces the
   direct-predecessor condition when a replica becomes readable; this
   invariant states its transitive consequence. *)
ApplicationLocalCausal ==
    \A a \in Apps :
       \A r \in Reps :
          \A t \in readable[a][r] :
             \A p \in Ancestors(a,NodeOf(r)) :
                 t \in readable[a][Chosen(a,p)]

(*
 * A waiting application can always acquire its next lock, unless that lock
 * is owned by another transaction.  This is the state predicate used to
 * derive deadlock freedom from the strict global lock order.
 *)
WaitingFor(a) ==
    active[a] /\
    pc[a] <= Len(NeedSeq(a)) /\
    NeedSeq(a)[pc[a]] \in Reps /\
    owner[NeedSeq(a)[pc[a]]] # Nil

WaitFor(a,b) ==
    WaitingFor(a) /\
    owner[NeedSeq(a)[pc[a]]] = <<b,txno[b]>>

RECURSIVE WaitReach(_,_)
WaitReach(a,b) ==
    WaitFor(a,b) \/
    \E c \in Apps : WaitFor(a,c) /\ WaitReach(c,b)

(* Section 5.3.1: acyclicity of the wait-for relation.  WaitingFor is
   the transaction blocked at its next ordered lock; WaitFor identifies
   the transaction currently owning that replica.  NoDeadlock excludes
   cycles in the transitive wait-for relation. *)
NoDeadlock ==
    ~ (\E a \in Apps : WaitReach(a,a))

TypeOK ==
    /\ active \in [Apps -> BOOLEAN]
    /\ txno \in [Apps -> Nat]
    /\ txTs \in [Apps -> Nat]
    /\ pc \in [Apps -> 1..(Cardinality(Reps)+1)]
    /\ held \in [Apps -> SUBSET Reps]
    /\ owner \in [Reps -> ((Apps \times Nat) \cup {Nil})]
    /\ written \in [Apps -> SUBSET Nodes]
    /\ versions \in [Reps -> SUBSET Nat]
    /\ pending \in [Reps -> SUBSET Nat]
    /\ nextTs \in Nat

=============================================================================
