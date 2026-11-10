from __future__ import annotations


class ExecutionError(RuntimeError):
    """Base class for execution-layer failures."""


class ContractQualificationError(ExecutionError):
    """A broker has definitively rejected an instrument contract."""


class IbkrExecutionError(ExecutionError):
    """IBKR could not safely complete a requested execution action."""


class ConfirmedNewOrderRejection(IbkrExecutionError):
    """Adapter proved a new timed-exit refusal with unchanged holdings/protection."""

    def __init__(
        self, *, order_id: int, order_ref: str, symbol: str, quantity: int,
        client_id: int, contract_id: int, detail: str, submitted_at: str,
        executions_checked_at: str, verified_at: str,
    ) -> None:
        super().__init__(f"IBKR_CONFIRMED_NEW_ORDER_REJECTED: {detail}")
        self.order_id = order_id
        self.order_ref = order_ref
        self.symbol = symbol
        self.quantity = quantity
        self.client_id = client_id
        self.contract_id = contract_id
        self.detail = detail
        self.submitted_at = submitted_at
        self.executions_checked_at = executions_checked_at
        self.verified_at = verified_at


class IbkrContractQualificationError(IbkrExecutionError, ContractQualificationError):
    """IBKR could not resolve a requested instrument to a tradable contract."""
