import asyncio
from typing import Callable, Dict, List
from trader.base.event import Event

class EventEngine:
    def __init__(self):
        self._queue = asyncio.Queue()
        self._handlers: Dict[str, List[Callable]] = {}
        self._general_handlers: List[Callable] = []

    async def start(self):
        """启动事件引擎主循环"""
        while True:
            event = await self._queue.get()
            await self._process_event(event)

    async def _process_event(self, event: Event):
        """处理单个事件并调用注册的处理器"""
        # 调用特定事件类型的处理器
        if event.event_type in self._handlers:
            for handler in self._handlers[event.event_type]:
                await handler(event)
        
        # 调用通用处理器
        for handler in self._general_handlers:
            await handler(event)

    def register(self, event_type: str, handler: Callable):
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)

    def unregister(self, event_type: str, handler: Callable):
        if event_type in self._handlers and handler in self._handlers[event_type]:
            self._handlers[event_type].remove(handler)

    def put(self, event: Event):
        asyncio.create_task(self._queue.put(event))

    def register_general(self, handler: Callable):
        if handler not in self._general_handlers:
            self._general_handlers.append(handler)

    def unregister_general(self, handler: Callable):
        if handler in self._general_handlers:
            self._general_handlers.remove(handler)