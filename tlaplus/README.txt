The TLA+ tools require Java 11+ to run.

Files:
- DRepsLocal.tla ....... Abstract finite-state model
- MC_DRepsLocal.tla .... First instantiation of the DRepsLocal.tla
- MC_DRepsLocal.cfg .... TLC configutation for the MC_DRepsLocal.tla
- MC_DRepsLocal2.tla ... Second instantiation of the DRepsLocal.tla
- MC_DRepsLocal2.cfg ... TLC configuration for the MC_DRepsLocal2.tla
- MC_DRepsLocal3.tla ... Third instantiation of the DRepsLocal.tla
- MC_DRepsLocal3.cfg ... TLC configuration for the MC_DRepsLocal3.tla

Usage:
java -jar tla2tools.jar MC_DRepsLocal  # TLC model-checking for the first model
java -jar tla2tools.jar MC_DRepsLocal2 # TLC model-checking for the second model
java -jar tla2tools.jar MC_DRepsLocal3 # TLC model-checking for the third model
