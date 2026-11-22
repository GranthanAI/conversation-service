"""Share links (Q3). All routes need a JWT except GET /shares/public/{token}, which is public and read-only."""

from datetime import datetime
from typing import List, Literal, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel

from app.api.deps import get_conversation_service, get_share_service
from app.exports.collector import SourceMissing
from app.security import CurrentUser, get_current_user
from app.services.conversation_service import ConversationService
from app.services.share_service import RateLimited, ShareNotFound, ShareRefused, ShareService

router = APIRouter()


class CreateShareRequest(BaseModel):
    conversation_id: UUID
    scope: Literal["conversation", "hierarchy"]


class ShareCreated(BaseModel):
    share_id: UUID
    url_token: str
    scope: str
    title: str
    message_count: int
    created_at: datetime


class ShareItem(BaseModel):
    share_id: UUID
    conversation_id: UUID
    scope: str
    title: str
    message_count: int
    created_at: datetime
    refreshed_at: datetime
    view_count: int = 0


class ShareRefreshed(BaseModel):
    share_id: UUID
    scope: str
    title: str
    message_count: int
    created_at: datetime
    refreshed_at: datetime


class ShareList(BaseModel):
    items: List[ShareItem]


_REFUSALS = {
    "temporary_conversation": "Temporary chats can't be shared.",
    "share_limit": "You have reached the number of shared links your account can keep. Delete one to make another.",
    "share_too_large": "This is too large to share as one link.",
}


def _refused(e: ShareRefused) -> HTTPException:
    return HTTPException(status_code=e.status, detail={"code": e.code, "message": _REFUSALS.get(e.code, e.code)})


def _not_found() -> HTTPException:
    return HTTPException(status_code=404, detail={"code": "share_not_found", "message": "This link was removed or never existed."})


@router.post("", response_model=ShareCreated, status_code=status.HTTP_201_CREATED, summary="Create a share link")
async def create_share(
    body: CreateShareRequest,
    user: CurrentUser = Depends(get_current_user),
    conversations: ConversationService = Depends(get_conversation_service),
    service: ShareService = Depends(get_share_service),
):
    conv = await conversations.get(body.conversation_id)
    if conv is None or getattr(conv.status, "value", conv.status) == "deleted":
        raise HTTPException(status_code=404, detail="Conversation not found")
    if conv.user_id != user.id:
        raise HTTPException(status_code=403, detail="Forbidden: You do not own this conversation")
    try:
        return await service.create(user.id, conv, body.scope)
    except ShareRefused as e:
        raise _refused(e)
    except SourceMissing:
        raise HTTPException(status_code=404, detail="Conversation not found")


@router.get("", response_model=ShareList, summary="My active share links (no tokens)")
async def list_shares(user: CurrentUser = Depends(get_current_user), service: ShareService = Depends(get_share_service)):
    return ShareList(items=await service.list(user.id))


@router.post("/{share_id}/refresh", response_model=ShareRefreshed, summary="Rebuild the snapshot under the same link")
async def refresh_share(share_id: UUID, user: CurrentUser = Depends(get_current_user),
                        service: ShareService = Depends(get_share_service)):
    try:
        return await service.refresh(user.id, share_id)
    except (ShareNotFound, SourceMissing):
        raise _not_found()
    except ShareRefused as e:
        raise _refused(e)


@router.delete("/{share_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Revoke a share link")
async def revoke_share(share_id: UUID, user: CurrentUser = Depends(get_current_user),
                       service: ShareService = Depends(get_share_service)):
    await service.revoke(user.id, share_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


def client_ip(request: Request) -> str:
    """The caller's address as the gateway reports it (first X-Forwarded-For entry); the socket peer otherwise."""
    forwarded = request.headers.get("x-forwarded-for", "")
    first = forwarded.split(",")[0].strip()
    if first:
        return first
    return request.client.host if request.client else "unknown"


@router.get("/public/{token}", summary="PUBLIC: read a shared snapshot (no JWT)")
async def read_public(token: str, request: Request, service: ShareService = Depends(get_share_service)):
    try:
        await service.rate_check(client_ip(request))
    except RateLimited as e:
        raise HTTPException(status_code=429, detail={"code": "rate_limited", "message": "Too many requests."},
                            headers={"Retry-After": str(max(1, e.retry_after))})
    try:
        body = await service.read_public(token)
    except ShareNotFound:
        raise _not_found()
    return Response(content=body, media_type="application/json",
                    headers={"Cache-Control": "private, max-age=60", "X-Robots-Tag": "noindex"})
