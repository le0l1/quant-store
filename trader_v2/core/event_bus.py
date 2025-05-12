# event_bus.py
import asyncio
import logging
from typing import Dict, List, Callable, Type, Any

from core.events import Event # 导入我们定义的事件类

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EventBus:
    def __init__(self):
        self._queue: asyncio.Queue[Event] = asyncio.Queue()
        self._listeners: Dict[Type, List[Callable[[Event], Any]]] = {}
        self._processing_tasks: set[asyncio.Task] = set()
        self._running = False

    def subscribe(self, event_type: Type[Event], handler: Callable[[Event], Any]):
        if not asyncio.iscoroutinefunction(handler):
             logger.warning(f"Handler for {event_type.__name__} is not a coroutine function. It might block the event loop.")

        if event_type not in self._listeners:
            self._listeners[event_type] = []
            logger.info(f"Created subscription list for event type: {event_type.__name__}")

        if handler not in self._listeners[event_type]:
            self._listeners[event_type].append(handler)
            logger.info(f"Subscribed handler {handler.__name__} to event type: {event_type.__name__}")
        else:
            logger.warning(f"Handler {handler.__name__} already subscribed to {event_type.__name__}")


    def publish(self, event: Event):
        self._queue.put_nowait(event)


    async def _dispatch_event(self, event: Event):
        listeners = self._listeners.get(type(event), [])
        if not listeners:
            return

        for handler in listeners:
            await handler(event)

    async def run(self):
        logger.info("Event Bus started.")
        self._running = True
        while self._running:
            try:
                event = await self._queue.get()
                logger.debug(f"Processing event: {event.type} (ID: {event.id})")
                await self._dispatch_event(event)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Error processing event: {e}")
                self._queue.task_done() 

    async def stop(self):
        logger.info("Stopping Event Bus. Waiting for queue to empty...")
        self._running = False # Signal run loop to exit after processing current queue
        await self._queue.join()


    async def wait_until_queue_empty(self):
        await self._queue.join()