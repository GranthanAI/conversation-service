"""An in-process fake of billing's BillingInternal gRPC server (no real billing needed)."""

import asyncio
from typing import Callable, Optional

import grpc

from app.clients.billing_grpc import billing_pb2 as pb
from app.clients.billing_grpc import billing_pb2_grpc as pb_grpc


class FakeBilling(pb_grpc.BillingInternalServicer):
    def __init__(self):
        self.reserve_calls: list = []
        self.release_calls: list = []
        self.entitlement_calls: list = []
        self.metadata: list = []
        self.delay = 0.0
        self.fail_code: Optional[grpc.StatusCode] = None
        self.reserve_handler: Optional[Callable] = None     # (request) -> CheckAndReserveResponse
        self.release_fail_times = 0
        self.entitlements = pb.Entitlements(user_id="u", plan_code="pro", source="subscription",
                                            allowed_models=["openai/gpt-oss-20b"], limits={"messages_per_day": 60})

    async def _gate(self, context):
        self.metadata.append(dict(context.invocation_metadata()))
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail_code is not None:
            await context.abort(self.fail_code, "forced failure")

    async def CheckAndReserve(self, request, context):
        self.reserve_calls.append(request)
        await self._gate(context)
        if self.reserve_handler:
            return self.reserve_handler(request)
        return pb.CheckAndReserveResponse(allowed=True, hold_id="hold-1", granted_group=pb.MODEL_GROUP_FAST,
                                          remaining_today=24, funded_by="plan", max_context_tokens=16000)

    async def Release(self, request, context):
        self.release_calls.append(request)
        if self.release_fail_times > 0:
            self.release_fail_times -= 1
            await context.abort(grpc.StatusCode.UNAVAILABLE, "down")
        await self._gate(context)
        return pb.ReleaseResponse(released=True)

    async def GetEntitlements(self, request, context):
        self.entitlement_calls.append(request)
        await self._gate(context)
        return self.entitlements


async def start_fake_billing(fake: FakeBilling):
    server = grpc.aio.server()
    pb_grpc.add_BillingInternalServicer_to_server(fake, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, f"127.0.0.1:{port}"
