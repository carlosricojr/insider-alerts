from __future__ import annotations


class ExecutionError(RuntimeError):
    """Base class for execution-layer failures."""


class ContractQualificationError(ExecutionError):
    """A broker has definitively rejected an instrument contract."""


class IbkrExecutionError(ExecutionError):
    """IBKR could not safely complete a requested execution action."""


class IbkrContractQualificationError(IbkrExecutionError, ContractQualificationError):
    """IBKR could not resolve a requested instrument to a tradable contract."""
