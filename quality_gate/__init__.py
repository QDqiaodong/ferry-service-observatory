"""Reproducible quality gate for the ferry revision state machine.

This package is an *independent* checker. It never imports
``ferry_observer``: the UTC normalization, JSONL/CSV parsing, lifecycle,
ledger accounting and dual-cutoff reporting below are re-implemented from the
written contract in ``docs/event-contract.md``. The gate drives the real
production package through its public API and compares every observable
outcome against this in-memory oracle.

Modules:

* ``canonical`` -- own timestamp/canonicalization/parsing primitives.
* ``oracle`` -- independent in-memory revision ledger and report model.
* ``generator`` -- fixed-seed interleaved history generator (no production
  functions are called).
* ``harness`` -- batch-by-batch comparison, full-table snapshots and the
  dual-cutoff/byte-stability checks.
* ``mutator`` -- isolated AST-level fault injection for mutation testing.
* ``replay`` / ``run_gate`` -- offline CLIs for singleton replay and the
  full gate run.
"""

GATE_BUNDLE_SCHEMA = "ferry-qg-bundle/1"
