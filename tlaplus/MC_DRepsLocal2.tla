---- MODULE MC_DRepsLocal2 ----

EXTENDS DRepsLocal

(*
 * FINITE INSTANTIATION OF THE PAPER'S MODEL (Sections 4-5)
 *
 * This module instantiates the generic DRepsLocal operators.  The symbols
 * below are not new protocol steps: they provide one concrete global DAG,
 * its application-local projections, replica sets, selected replicas,
 * consistency annotations, and the total order required by DRepsLocal.
 *
 * Global DAG: n1 -> n3 -> n4.
 *             n2 ->
 *
 * Applications a1 and a2:
 * a1 uses all four data sources; a2 uses n2, n3 and n4.  Each node has two
 * physical replicas.  Each application selects exactly one replica per
 * visible node.
 *
 * The chosen levels instantiate mixed consistency: a1 uses SER on n1,n2,n3
 * and CAUSAL on n4; a2 uses SER on n2 and CAUSAL on n3,n4.  This satisfies
 * the paper's edge constraint that a source cannot be weaker than its
 * dependent node.
 *
 * MCOrderIndex is intended to enumerate the total order defined in
 * DRepsLocal.ReplicaBefore: decreasing node depth, then NodeId, and
 * ReplicaId within a node.
 *)

CONSTANTS
    a1, a2,
    n1, n2, n3, n4,
    r1, r2, r3, r4, r5, r6, r7, r8

MCApps == {a1, a2}
MCNodes == {n1, n2, n3, n4}
MCReps == {r1, r2, r3, r4, r5, r6, r7, r8}

(* global DAG *)
MCEdge ==
    {<<n1,n3>>, <<n2,n3>>, <<n3,n4>>}

MCDepth ==
    [x \in MCNodes |->
        IF x = n1 \/ x = n2 THEN 2
        ELSE IF x = n3 THEN 1
        ELSE 0]

MCNodeId ==
    [x \in MCNodes |->
        IF x = n1 THEN 1
        ELSE IF x = n2 THEN 2
        ELSE IF x = n3 THEN 3
        ELSE 4]

MCReplicaId ==
    [x \in MCReps |->
        IF x = r1 THEN 1
        ELSE IF x = r2 THEN 2
        ELSE IF x = r3 THEN 1
        ELSE IF x = r4 THEN 2
        ELSE IF x = r5 THEN 1
        ELSE IF x = r6 THEN 2
        ELSE IF x = r7 THEN 1
        ELSE 2]

MCPhys ==
    [x \in MCReps |->
        IF x = r1 \/ x = r2 THEN n1
        ELSE IF x = r3 \/ x = r4 THEN n2
        ELSE IF x = r5 \/ x = r6 THEN n3
        ELSE n4]

MCReplicasOf ==
    [x \in MCNodes |->
        IF x = n1 THEN {r1,r2}
        ELSE IF x = n2 THEN {r3,r4}
        ELSE IF x = n3 THEN {r5,r6}
        ELSE {r7,r8}]

(* projection of the global DAG to each application's visible data sources *)
MCVisible ==
    [x \in MCApps |->
        IF x = a1 THEN {n1,n2,n3,n4}
        ELSE {n2,n3,n4}]

(* per-application surrogate consistency annotations *)
MCLevel ==
    [a \in MCApps |->
        IF a = a1
        THEN
            [n \in MCNodes |->
                IF n = n1 \/ n = n2 \/ n = n3
                THEN SER
                ELSE CAUSAL]
        ELSE
            [n \in MCNodes |->
                IF n = n2
                THEN SER
                ELSE CAUSAL]]

(* the replica selected by each application for each visible data source *)
MCAppRep ==
    [a \in MCApps |->
        IF a = a1
        THEN
            [n \in MCNodes |->
                IF n = n1 THEN r1
                ELSE IF n = n2 THEN r3
                ELSE IF n = n3 THEN r5
                ELSE r7]
        ELSE
            [n \in MCNodes |->
                IF n = n2 THEN r4
                ELSE IF n = n3 THEN r6
                ELSE r8]]

(* explicit enumeration of the global lock order (checked by OrderInvariant in the generic DrepsLocal.tla) *)
MCOrderIndex ==
    [x \in MCReps |->
        IF x = r1 THEN 1
        ELSE IF x = r2 THEN 2
        ELSE IF x = r3 THEN 3
        ELSE IF x = r4 THEN 4
        ELSE IF x = r5 THEN 5
        ELSE IF x = r6 THEN 6
        ELSE IF x = r7 THEN 7
        ELSE 8]

====