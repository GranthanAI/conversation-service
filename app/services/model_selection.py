"""Which model a send/regenerate asks billing for (integration plan P6, task 3).

  request.model given            -> that choice, STRICT: if billing says model_not_in_plan the user gets the error.
  request.model omitted          -> the saved default_model. If billing denies THAT with model_not_in_plan, the send is
                                    retried once as Auto and `defaults_applied.model` is set to true.
  "auto" (request or default)    -> requested_model None; billing picks inside the plan's default group.

Only the FORMAT of a model id is checked here ([a-z0-9./-]{1,64}); billing decides whether the plan includes it.
"""

from dataclasses import dataclass
from typing import Awaitable, Callable, Optional, Tuple

from app.clients.billing_client import ReserveResult


@dataclass(frozen=True)
class ModelChoice:
    requested_model: Optional[str]   # None = Auto
    from_default: bool               # True when it came from the saved default_model (not from the request)


def choose_model(request_model: Optional[str], default_model: Optional[str]) -> ModelChoice:
    if request_model is not None:
        return ModelChoice(None if request_model == "auto" else request_model, False)
    default = default_model or "auto"
    return ModelChoice(None if default == "auto" else default, True)


async def reserve_with_default_fallback(
    choice: ModelChoice, reserve: Callable[[Optional[str]], Awaitable[ReserveResult]],
) -> Tuple[ReserveResult, ModelChoice, bool]:
    """Calls `reserve(model)`; a denied SAVED default (model_not_in_plan) is retried once as Auto.
    Returns (result, the choice that was finally used, whether the fallback happened)."""
    result = await reserve(choice.requested_model)
    if (not result.allowed and result.deny_reason == "model_not_in_plan"
            and choice.from_default and choice.requested_model is not None):
        auto = ModelChoice(None, True)
        return await reserve(None), auto, True
    return result, choice, False
