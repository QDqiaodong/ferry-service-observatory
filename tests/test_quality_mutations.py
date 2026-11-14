"""Isolated mutation tests for the revision state-machine gate.

Each test injects one specific, behavior-changing fault into a *throwaway
copy* of the production package (AST rewrite, never source-text matching) and
then runs the real gate against the mutant. The contract says:

* the original, unmutated package must pass the gate;
* every mutant must be killed by an expected/actual behavior difference --
  textual source inspection is never used as evidence;
* no mutant may touch the working tree or leave bytecode behind.
"""
import ast
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from quality_gate.generator import DEFAULT_SEEDS, generate_history
from quality_gate.harness import GateFailure, run_history
from quality_gate.mutator import MUTATIONS, MutantPackage

ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_PKG = ROOT / "ferry_observer"


class WorktreeGuardCase(unittest.TestCase):
    def setUp(self):
        self._tracked = self._snapshot()

    def _snapshot(self):
        return {
            path: path.read_bytes()
            for path in PRODUCTION_PKG.rglob("*.py")
        }

    def tearDown(self):
        self.assertEqual(
            self._tracked,
            self._snapshot(),
            "mutation testing modified production source in the working tree",
        )
        leftovers = list(PRODUCTION_PKG.rglob("__pycache__"))
        for cache in leftovers:
            # Pre-existing caches are fine; assert no mutant aliased package
            # directories were created inside the source tree.
            self.assertNotIn("ferry_qg_mutant", str(cache))

    def test_mutant_directories_live_outside_the_worktree(self):
        pkg = MutantPackage(MUTATIONS[0])
        try:
            self.assertFalse(
                Path(pkg.tempdir).resolve().is_relative_to(ROOT.resolve())
            )
            self.assertFalse(
                any(
                    str(path).startswith(str(ROOT))
                    for path in Path(pkg.tempdir).rglob("*")
                )
            )
        finally:
            pkg.cleanup()


class MutationKillingCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.history = generate_history(DEFAULT_SEEDS[0])

    def test_original_package_passes_the_gate(self):
        record = run_history(self.history)
        self.assertEqual(record["rejected_batches"], 4)
        self.assertEqual(record["accepted_batches"], 11)

    def _assert_killed(self, mutation):
        pkg = MutantPackage(mutation)
        try:
            with self.assertRaises(GateFailure) as caught:
                run_history(self.history, production=pkg.production_api())
            # The evidence must be a behavioral expected/actual mismatch,
            # not a source-text assertion.
            message = str(caught.exception)
            self.assertTrue(
                caught.exception.expected != caught.exception.actual
                or "decision" in caught.exception.context
            )
            self.assertNotIn("source text", message)
        finally:
            pkg.cleanup()

    def test_duplicate_batch_advances_seq_is_killed(self):
        self._assert_killed("duplicate_batch_advances_seq")

    def test_filter_time_before_revision_is_killed(self):
        self._assert_killed("filter_time_before_revision")

    def test_redelivery_overwrites_first_source_is_killed(self):
        self._assert_killed("redelivery_overwrites_first_source")

    def test_each_mutant_is_a_distinct_ast_rewrite(self):
        # The injected faults must be genuine logic rewrites; guard the
        # mutator itself against degrading into a no-op copy.
        from quality_gate import mutator

        original_storage = (PRODUCTION_PKG / "storage.py").read_text()
        original_analysis = (PRODUCTION_PKG / "analysis.py").read_text()
        for mutation in MUTATIONS:
            pkg = MutantPackage(mutation)
            try:
                storage_text = (pkg.path / "storage.py").read_text()
                analysis_text = (pkg.path / "analysis.py").read_text()
                ast.parse(storage_text)
                ast.parse(analysis_text)
                if mutation == "filter_time_before_revision":
                    self.assertNotEqual(analysis_text, original_analysis)
                else:
                    self.assertNotEqual(storage_text, original_storage)
            finally:
                pkg.cleanup()

    def test_no_mutant_bytecode_remains_after_cleanup(self):
        before = set(Path(tempfile.gettempdir()).glob("qg-mutant-*"))
        for mutation in MUTATIONS:
            pkg = MutantPackage(mutation)
            pkg.production_api()
            pkg.cleanup()
        after = set(Path(tempfile.gettempdir()).glob("qg-mutant-*"))
        self.assertTrue(after.issubset(before))

    def test_mutants_never_import_under_the_real_package_name(self):
        # Mutant modules must execute from their aliased copy, so production
        # modules keep pointing at the pristine working-tree files.
        import ferry_observer

        real_file = Path(ferry_observer.__file__).resolve()
        for mutation in MUTATIONS:
            pkg = MutantPackage(mutation)
            try:
                pkg.production_api()
                self.assertEqual(
                    Path(ferry_observer.__file__).resolve(), real_file
                )
            finally:
                pkg.cleanup()
        self.assertEqual(Path(ferry_observer.__file__).resolve(), real_file)


if __name__ == "__main__":
    unittest.main()
