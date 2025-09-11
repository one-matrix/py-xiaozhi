# pip install fastapi uvicorn[standard] websockets
import asyncio
import json
import os
from collections import defaultdict, deque
from typing import Deque, Dict, Set

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse

app = FastAPI(title="DyCast Hub")

# 环境配置
HUB_KEY = os.getenv("HUB_KEY", "")                 # 共享密钥（留空则不鉴权）
MAX_BUF = int(os.getenv("MAX_BUF", "200"))         # 每个房间的历史缓存条数
UPSTREAM_WS = os.getenv("UPSTREAM_WS", "")         # 可选：再转发到上游 ws://... 或 wss://...

# 房间 -> 订阅者集合
subs: Dict[str, Set[WebSocket]] = defaultdict(set)
# 房间 -> 最近消息环形缓存
buffers: Dict[str, Deque[str]] = defaultdict(lambda: deque(maxlen=MAX_BUF))
# 并发保护
subs_lock = asyncio.Lock()

# 上游再转发队列（可选）
upstream_queue: "asyncio.Queue[str]" = asyncio.Queue()
upstream_task: asyncio.Task | None = None


@app.get("/health")
async def health():
    return PlainTextResponse("ok")


def authorized(key: str | None) -> bool:
    return (HUB_KEY == "") or (key == HUB_KEY)


async def _broadcast(room: str, data: str):
    """缓存 + 广播给该房间的所有订阅者 + 可选再转发到上游"""
    buffers[room].append(data)

    async with subs_lock:
        targets = list(subs[room])

    # 并发发送
    send_tasks = [ws.send_text(data) for ws in targets]
    results = await asyncio.gather(*send_tasks, return_exceptions=True)

    # 清理已断开的连接
    async with subs_lock:
        for ws, res in zip(targets, results):
            if isinstance(res, Exception):
                subs[room].discard(ws)

    # 可选：再推给上游
    if UPSTREAM_WS:
        try:
            upstream_queue.put_nowait(data)
        except asyncio.QueueFull:
            # 队列满了就丢弃（也可以改成丢最旧的）
            pass


@app.websocket("/ingest")
async def ingest(
    websocket: WebSocket,
    room: str = Query(..., description="房间号"),
    key: str | None = Query(None, description="共享密钥"),
):
    """dycast 填这个地址用来写入：/ingest?room=xxx&key=..."""
    if not authorized(key):
        await websocket.close(code=1008)  # Policy Violation
        return

    await websocket.accept()
    try:
        while True:
            msg = await websocket.receive()

            if msg.get("text") is not None:
                data = msg["text"]
            elif msg.get("bytes") is not None:
                # 尝试按 UTF-8 文本解析；如果你真的需要二进制，可自行改造
                try:
                    data = msg["bytes"].decode("utf-8", errors="strict")
                except Exception:
                    # 不可解码时，避免把二进制直接广播
                    data = json.dumps({"_bin": True})
            else:
                continue

            await _broadcast(room, data)

    except WebSocketDisconnect:
        # 写入端断开，静默结束
        pass


async def _heartbeat(ws: WebSocket, interval: float = 30.0):
    """给订阅端发心跳，帮助穿越部分中间层的空闲超时"""
    try:
        while True:
            await asyncio.sleep(interval)
            # 发送一个轻量心跳；客户端可忽略
            await ws.send_text('{"_type":"ping"}')
    except Exception:
        pass  # 由订阅协程统一清理


@app.websocket("/sub")
async def subscribe(
    websocket: WebSocket,
    room: str = Query("default", description="房间号"),
    key: str | None = Query(None, description="共享密钥"),
    replay: int = Query(0, ge=0, le=MAX_BUF, description="连接时回放最近 N 条"),
):
    """设备/服务订阅：/sub?room=xxx&key=...&replay=50"""
    if not authorized(key):
        await websocket.close(code=1008)
        return

    await websocket.accept()

    # 注册到订阅者
    async with subs_lock:
        subs[room].add(websocket)

    # 可选：回放最近 N 条
    if replay:
        for item in list(buffers[room])[-replay:]:
            try:
                await websocket.send_text(item)
            except Exception:
                pass

    # 启动心跳任务
    hb_task = asyncio.create_task(_heartbeat(websocket))

    try:
        # 保持连接直到对端断开；不强制接收内容
        while True:
            # 等待对端任意消息（包括 ping/pong）；若对端不发，心跳也能保持多数网关
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        hb_task.cancel()
        async with subs_lock:
            subs[room].discard(websocket)


@app.on_event("startup")
async def _start_upstream():
    """上游转发任务（可选）"""
    if not UPSTREAM_WS:
        return

    async def worker():
        import websockets  # websockets==12+

        backoff = 1
        while True:
            try:
                async with websockets.connect(UPSTREAM_WS, max_queue=None) as ws:
                    backoff = 1
                    while True:
                        data = await upstream_queue.get()
                        await ws.send(data)
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    global upstream_task
    upstream_task = asyncio.create_task(worker())


if __name__ == "__main__":
    import os

    import uvicorn
    port = int(os.getenv("PORT", "8787"))
    uvicorn.run(app, host="0.0.0.0", port=port)
