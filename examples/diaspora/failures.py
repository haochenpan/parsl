"""Failure scenarios for the diaspora example CLI."""

from __future__ import annotations

import logging
from typing import Any

from parsl.app.app import python_app

from runtime import _log_common_workflow_start, execute_logged_command, loaded_parsl, make_config_for_mode

PYTHON_DIV_ZERO_SCENARIO = "python-div-zero"
PYTHON_MISSING_MODULE_SCENARIO = "python-missing-module"
PYTHON_PEP750_TSTRING_SCENARIO = "python-pep750-t-string"
PYTHON_CHAIN_SCENARIO = "python-chain"

SCENARIO_CHOICES = (
    PYTHON_DIV_ZERO_SCENARIO,
    PYTHON_MISSING_MODULE_SCENARIO,
    PYTHON_PEP750_TSTRING_SCENARIO,
    PYTHON_CHAIN_SCENARIO,
)


@python_app
def divide_by_zero():
    return 1 / 0


@python_app
def missing_module_import():
    import non_exist_module  # type: ignore[import-not-found]

    return non_exist_module


@python_app
def pep750_tstring_feature():
    namespace = {"name": "diaspora"}
    exec('template = t"Hello {name}"', namespace, namespace)
    return namespace["template"]


@python_app
def chain_root_fails():
    raise ValueError("Deliberate failure")


@python_app
def chain_depends(parent):
    return 1


def _run_python_div_zero(_args, logger: logging.Logger, _dfk: Any) -> None:
    future = divide_by_zero()
    try:
        future.result()
        logger.error("Unexpected success")
    except Exception as exc:
        logger.info("Expected failure: %s %s", type(exc).__name__, exc)


def _run_python_missing_module(_args, logger: logging.Logger, _dfk: Any) -> None:
    future = missing_module_import()
    try:
        future.result()
        logger.error("Unexpected success")
    except Exception as exc:
        logger.info("Expected permanent environment failure: %s %s", type(exc).__name__, exc)


def _run_python_pep750_tstring(_args, logger: logging.Logger, _dfk: Any) -> None:
    future = pep750_tstring_feature()
    try:
        future.result()
        logger.error("Unexpected success")
    except Exception as exc:
        logger.info(
            "Expected Python-version failure (PEP 750 t-strings require 3.14): %s %s",
            type(exc).__name__,
            exc,
        )


def _run_python_chain(_args, logger: logging.Logger, _dfk: Any) -> None:
    root = chain_root_fails()
    f2 = chain_depends(root)
    f3 = chain_depends(f2)
    f4 = chain_depends(f3)

    for name, future in (("f1", root), ("f2", f2), ("f3", f3), ("f4", f4)):
        try:
            result = future.result()
            logger.info("%s success: %s", name, result)
        except Exception as exc:
            logger.info("%s failed as expected: %s %s", name, type(exc).__name__, exc)


SCENARIOS = {
    PYTHON_DIV_ZERO_SCENARIO: _run_python_div_zero,
    PYTHON_MISSING_MODULE_SCENARIO: _run_python_missing_module,
    PYTHON_PEP750_TSTRING_SCENARIO: _run_python_pep750_tstring,
    PYTHON_CHAIN_SCENARIO: _run_python_chain,
}


def run_failure_scenario(args) -> int:
    if args.scenario not in SCENARIOS:
        supported = ", ".join(SCENARIO_CHOICES)
        raise ValueError(f"Unknown --scenario {args.scenario!r}; supported: {supported}")

    if args.count < 1:
        raise ValueError("--count must be >= 1")

    scenario_runner = SCENARIOS[args.scenario]

    def _run(context) -> None:
        logger = context.logger
        _log_common_workflow_start(context, logger)
        logger.info("Debug logging is forced to DEBUG for this failure command.")

        config = make_config_for_mode(context.args.mode, context.args.count, logger)
        with loaded_parsl(config) as dfk:
            logger.info("Parsl loaded. run_id=%s", getattr(dfk, "run_id", "<unknown>"))
            scenario_runner(context.args, logger, dfk)

    scenario_logger_name = args.scenario.replace("-", "_")
    return execute_logged_command(
        args,
        logger_name=f"parsl.examples.diaspora.failure.{scenario_logger_name}.{args.mode}",
        command=_run,
        force_debug=True,
    )
