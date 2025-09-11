import asyncio
import json
import time
from typing import Optional

import websockets

from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class WssSubscriber:
    """
    WSS订阅器，用于接收wss.py服务器的广播消息.
    """

    def __init__(
        self,
        url: str = "ws://127.0.0.1:8787/sub",
        send_interval: int = 10,
        batch_size: int = 3,
    ):
        self.url = url
        self.send_interval = send_interval  # TTS发送间隔（秒）
        self.batch_size = batch_size  # 批量发送大小
        self.running = False
        self.subscribe_task: Optional[asyncio.Task] = None
        self.scheduler_task: Optional[asyncio.Task] = None
        self.message_queue = asyncio.Queue()
        self.app = None
        self.last_send_time = 0

    def set_application(self, app):
        """
        设置Application实例引用.
        """
        self.app = app

    async def start(self):
        """
        启动WSS订阅和TTS调度任务.
        """
        if self.running:
            logger.warning("WSS订阅器已在运行")
            return

        self.running = True
        logger.info(f"启动WSS订阅器，URL: {self.url}")

        # 启动订阅任务
        self.subscribe_task = asyncio.create_task(self._subscribe_loop())
        # 启动TTS调度任务
        self.scheduler_task = asyncio.create_task(self._tts_scheduler_loop())

    async def stop(self):
        """
        停止WSS订阅和调度任务.
        """
        if not self.running:
            return

        logger.info("正在停止WSS订阅器...")
        self.running = False

        # 取消任务
        if self.subscribe_task:
            self.subscribe_task.cancel()
            try:
                await self.subscribe_task
            except asyncio.CancelledError:
                pass

        if self.scheduler_task:
            self.scheduler_task.cancel()
            try:
                await self.scheduler_task
            except asyncio.CancelledError:
                pass

        logger.info("WSS订阅器已停止")

    async def _subscribe_loop(self):
        """
        WSS订阅循环.
        """
        while self.running:
            try:
                logger.info(f"正在连接WSS服务器: {self.url}")
                async with websockets.connect(self.url) as ws:
                    logger.info("WSS连接成功")
                    async for raw_message in ws:
                        if not self.running:
                            break

                        logger.debug(f"收到WSS消息: {raw_message}")

                        # 将消息放入队列等待处理
                        try:
                            await self.message_queue.put(raw_message)
                        except Exception as e:
                            logger.error(f"消息入队失败: {e}")

            except websockets.exceptions.ConnectionClosed:
                logger.warning("WSS连接已关闭")
            except Exception as e:
                logger.error(f"WSS连接错误: {e}")

            if self.running:
                logger.info("5秒后重新连接WSS...")
                await asyncio.sleep(5)

    async def _tts_scheduler_loop(self):
        """
        TTS调度循环，智能等待TTS播报完成再发送下一次.
        """
        while self.running:
            try:
                # 等待指定间隔时间
                await asyncio.sleep(self.send_interval)

                # 检查是否有消息要处理
                messages_to_process = []
                message_count = 0

                # 收集队列中的消息，但限制批量大小
                while (
                    not self.message_queue.empty() and message_count < self.batch_size
                ):
                    try:
                        message = self.message_queue.get_nowait()
                        messages_to_process.append(message)
                        message_count += 1
                    except asyncio.QueueEmpty:
                        break

                if messages_to_process:
                    # 处理消息并生成TTS文本
                    tts_text = self._process_messages(messages_to_process)

                    if tts_text and self.app:
                        # 等待当前TTS播报完成
                        await self._wait_for_tts_completion()

                        # 如果队列中还有更多消息，显示提示
                        remaining = self.message_queue.qsize()
                        if remaining > 0:
                            logger.info(
                                f"发送TTS（处理了{len(messages_to_process)}条消息，还有{remaining}条待处理）: {tts_text}"
                            )
                        else:
                            logger.info(
                                f"发送TTS（处理了{len(messages_to_process)}条消息）: {tts_text}"
                            )

                        try:
                            await self.app._send_text_tts(tts_text)
                            self.last_send_time = time.time()
                        except Exception as e:
                            logger.error(f"发送TTS失败: {e}")
                else:
                    logger.debug(f"没有新消息，等待{self.send_interval}秒后再检查")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"TTS调度循环错误: {e}")

    async def _wait_for_tts_completion(self):
        """
        等待TTS播报完成.
        """
        if not self.app:
            return

        # 导入DeviceState
        from src.constants.constants import DeviceState

        # 如果正在播报，等待播报完成
        max_wait_time = 30  # 最大等待30秒，防止卡死
        wait_start = time.time()

        while (
            hasattr(self.app, "device_state")
            and self.app.device_state == DeviceState.SPEAKING
            and self.running
        ):

            # 检查是否超时
            if time.time() - wait_start > max_wait_time:
                logger.warning("等待TTS播报完成超时，继续下一次发送")
                break

            logger.debug("等待TTS播报完成...")
            await asyncio.sleep(0.5)  # 每0.5秒检查一次状态

    def _process_messages(self, messages) -> str:
        """
        处理消息列表，生成TTS文本.
        """
        processed_texts = []

        for raw_message in messages:
            try:
                # 跳过心跳消息
                if '{"_type":"ping"}' in raw_message:
                    continue

                # 解析JSON数据
                if raw_message.startswith("[") and raw_message.endswith("]"):
                    # 数组格式的消息
                    data_list = json.loads(raw_message)
                    for data in data_list:
                        text = self._format_single_message(data)
                        if text:
                            processed_texts.append(text)
                elif raw_message.startswith("{") and raw_message.endswith("}"):
                    # 单个对象消息
                    data = json.loads(raw_message)
                    text = self._format_single_message(data)
                    if text:
                        processed_texts.append(text)

            except json.JSONDecodeError:
                logger.debug(f"无法解析的消息: {raw_message}")
            except Exception as e:
                logger.error(f"处理消息失败: {e}")

        # 合并所有文本，根据批量大小进行智能合并
        if processed_texts:
            # 如果文本很多，优先选择重要的消息类型
            important_texts = []
            normal_texts = []

            for text in processed_texts:
                if any(keyword in text for keyword in ["送了", "礼物", "进入直播间"]):
                    important_texts.append(text)
                else:
                    normal_texts.append(text)

            # 优先使用重要消息，再补充普通消息
            final_texts = (
                important_texts[:2]
                + normal_texts[: max(1, self.batch_size - len(important_texts))]
            )

            if final_texts:
                result = "，".join(final_texts)
                # 适当增加长度限制，因为是批量处理
                return result[:150]  # 批量处理时允许更长的文本

        return ""

    def _format_single_message(self, data: dict) -> str:
        """
        格式化单条消息为TTS文本.
        """
        try:
            method = data.get("method", "")

            if method == "WebcastChatMessage":
                # 聊天消息
                user = data.get("user", {}).get("name", "用户")
                content = data.get("content", "")
                if content and content != "[捂脸][捂脸][捂脸]":  # 过滤表情
                    return f"{user}说{content}"

            elif method == "WebcastGiftMessage":
                # 礼物消息
                user = data.get("user", {}).get("name", "用户")
                gift = data.get("gift", {})
                gift_name = gift.get("name", "礼物")
                count = gift.get("count", "1")
                if count and int(count) > 0:
                    return f"{user}送了{count}个{gift_name}"

            elif method == "WebcastMemberMessage":
                # 进入直播间消息
                user = data.get("user", {}).get("name", "用户")
                return f"欢迎{user}进入直播间"

            elif method == "WebcastLikeMessage":
                # 点赞消息（频率太高，可选择性忽略）
                return ""

        except Exception as e:
            logger.debug(f"格式化消息失败: {e}")

        return ""
