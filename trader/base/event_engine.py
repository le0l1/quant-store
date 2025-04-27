# event_engine_async.py
import asyncio
import time
from typing import Callable, Dict, List, Optional, Awaitable # Added Awaitable
from collections import defaultdict
import logging
import inspect # To check if handler is coroutine function

from trader.base.event import Event


logger = logging.getLogger(__name__)

class EventEngine:
    """
    事件驱动引擎核心 - 基于 asyncio。
    负责事件的分发和处理。
    """
    def __init__(self, name: str = "DefaultEngine", mode: str = 'live', queue_timeout: float = 0.1):
        """
        初始化事件引擎。
        :param mode: 'live' (异步运行) 或 'backtest' (同步逻辑驱动)。
        """
        self._name = name
        self._queue = asyncio.Queue() # Use asyncio Queue
        self._active = False
        self._mode = mode.lower()
        # Task for the main loop in live mode
        self._main_loop_task: Optional[asyncio.Task] = None
        self._handlers: Dict[str, List[Callable[[Event], Awaitable[None]]]] = defaultdict(list)
        self._general_handlers: List[Callable[[Event], Awaitable[None]]] = []
        self._queue_timeout = queue_timeout # Timeout for getting items in live mode

        if self._mode not in ['live', 'backtest']:
            raise ValueError("模式必须是 'live' 或 'backtest'")

        logger.info(f"Async 事件引擎 '{self._name}' 初始化完成，模式: {self._mode}。")

    async def _process_event(self, event: Event):
        """处理单个事件，调用注册的异步处理器。"""
        # logger.debug(f"引擎 '{self._name}' 处理事件: {event}")
        tasks_to_await = [] # Collect tasks/coroutines for potential await

        # --- Dispatch to specific handlers ---
        if event.event_type in self._handlers:
            handlers_copy = self._handlers[event.event_type][:]
            for handler in handlers_copy:
                try:
                    if inspect.iscoroutinefunction(handler):
                        # If handler is async def, create task
                        tasks_to_await.append(asyncio.create_task(handler(event)))
                    else:
                        # If handler is a regular function, run it (might block loop briefly)
                        # For long-running sync handlers, consider run_in_executor
                        logger.debug(f"调用同步处理器 {getattr(handler, '__name__', 'unknown')}")
                        handler(event) # Direct call - blocks asyncio loop if long!
                except Exception as e:
                    logger.error(f"处理器 {getattr(handler, '__name__', 'unknown')} 处理事件 {event} 时出错: {e}", exc_info=True)

        # --- Dispatch to general handlers ---
        general_handlers_copy = self._general_handlers[:]
        for handler in general_handlers_copy:
             try:
                 if inspect.iscoroutinefunction(handler):
                     tasks_to_await.append(asyncio.create_task(handler(event)))
                 else:
                     logger.debug(f"调用同步通用处理器 {getattr(handler, '__name__', 'unknown')}")
                     handler(event) # Direct call
             except Exception as e:
                 logger.error(f"通用处理器 {getattr(handler, '__name__', 'unknown')} 处理事件 {event} 时出错: {e}", exc_info=True)

        # --- Await handler tasks ---
        # This ensures handlers complete before the next event might be processed *if* called sequentially
        if tasks_to_await:
            # Wait for all created handler tasks for this event to complete
            # Using return_exceptions=True prevents one failing handler from stopping others
            results = await asyncio.gather(*tasks_to_await, return_exceptions=True)
            # Log any exceptions returned by gather
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"事件处理协程中发生错误: {result}", exc_info=result)


    async def _run_live_loop(self):
        """主事件循环 (用于 live 模式，作为 asyncio Task 运行)。"""
        logger.info(f"Async 事件引擎 '{self._name}' 实时模式循环启动。")
        self._active = True
        while self._active:
            try:
                # Wait for an event with a timeout
                event = await asyncio.wait_for(self._queue.get(), timeout=self._queue_timeout)
                # Process the event and its handlers asynchronously
                # We don't necessarily wait for handlers here in live mode, allow concurrency
                asyncio.create_task(self._process_event(event))
                self._queue.task_done() # Signal that queue item processing has started/handled
            except asyncio.TimeoutError:
                # logger.debug("Queue timeout, checking active status.")
                continue # Just check self._active again
            except asyncio.CancelledError:
                logger.info("Live loop task cancelled.")
                break
            except Exception as e:
                logger.error(f"Async 事件引擎 '{self._name}' 实时模式运行中发生错误: {e}", exc_info=True)
                # Optional: Add a small delay to prevent tight error loops
                await asyncio.sleep(0.1)

        logger.info(f"Async 事件引擎 '{self._name}' 实时模式循环正常停止。")
        # Process remaining events after loop stops? Optional.
        logger.info("Processing any remaining events after stop signal...")
        while not self._queue.empty():
            try:
                 event = self._queue.get_nowait()
                 await self._process_event(event) # Await processing here
                 self._queue.task_done()
            except asyncio.QueueEmpty:
                 break
            except Exception as e:
                 logger.error(f"处理剩余事件时出错: {e}", exc_info=True)
        logger.info("Finished processing remaining events.")


    async def run_sync_cycle_async(self):
        """
        运行一个同步处理周期 (用于 backtest 模式)。
        处理队列中所有当前存在的事件，并等待其处理器完成。
        """
        if self._mode != 'backtest':
             logger.warning("run_sync_cycle_async 仅用于回测模式。")
             return

        # Process all events currently in the queue sequentially
        while not self._queue.empty():
             try:
                 event = self._queue.get_nowait() # Non-blocking get
                 # Process event AND await its handlers completion
                 await self._process_event(event)
                 self._queue.task_done() # Mark task done AFTER processing complete
             except asyncio.QueueEmpty:
                 break
             except Exception as e:
                 logger.error(f"Async 事件引擎 '{self._name}' 同步处理周期中发生错误: {e}", exc_info=True)


    # Register expects handlers to be awaitable (async def) or regular functions
    def register(self, event_type: Optional[str], handler: Callable[[Event], Awaitable[None]]):
        """注册事件处理器 (可以是 async def 或普通函数)。"""
        if not callable(handler): logger.error(f"注册失败：处理器 {handler} 不可调用。"); return

        # Optional: Check if handler is async or sync here if needed
        # is_async = inspect.iscoroutinefunction(handler)
        # logger.debug(f"Registering {'async' if is_async else 'sync'} handler {getattr(handler,'__name__','unknown')}")

        if event_type is None:
            if handler not in self._general_handlers:
                self._general_handlers.append(handler)
                logger.info(f"通用处理器 {getattr(handler, '__name__', 'unknown')} 已注册。")
        else:
            if handler not in self._handlers[event_type]:
                self._handlers[event_type].append(handler)
                logger.info(f"处理器 {getattr(handler, '__name__', 'unknown')} 已注册监听 '{event_type}'。")


    def unregister(self, event_type: Optional[str], handler: Callable[[Event], Awaitable[None]]):
        # ... (unregister logic remains the same) ...
        if event_type is None:
            if handler in self._general_handlers: self._general_handlers.remove(handler); logger.info(f"通用处理器 {getattr(handler, '__name__', 'unknown')} 已注销。")
        else:
            if event_type in self._handlers and handler in self._handlers[event_type]: self._handlers[event_type].remove(handler); logger.info(f"处理器 {getattr(handler, '__name__', 'unknown')} 已从 '{event_type}' 注销。")


    def put(self, event: Event):
        """将事件放入异步队列 (非阻塞)。"""
        if not isinstance(event, Event): logger.error(f"尝试放入非 Event 对象: {event}"); return
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
             logger.error(f"事件队列已满，无法放入事件: {event}")
             # Handle queue full scenario? Maybe block or raise?

    async def start(self):
        """启动事件引擎。在 live 模式下启动主循环 Task。"""
        if not self._active:
            self._active = True # Mark active early
            if self._mode == 'live':
                 if self._main_loop_task is None or self._main_loop_task.done():
                     self._main_loop_task = asyncio.create_task(self._run_live_loop())
                     logger.info(f"Async 事件引擎 '{self._name}' (live 模式) 循环任务已启动。")
                 else:
                      logger.warning(f"Async 事件引擎 '{self._name}' (live) 循环任务已在运行。")
            else:
                 logger.info(f"Async 事件引擎 '{self._name}' (backtest 模式) 已标记为活动状态。")


    async def stop(self):
        """停止事件引擎。在 live 模式下取消主循环 Task 并等待。"""
        if self._active:
            self._active = False # Signal loops to stop
            logger.info(f"正在停止 Async 事件引擎 '{self._name}'...")
            if self._mode == 'live' and self._main_loop_task:
                logger.info("等待 live 模式循环任务结束...")
                try:
                    # Wait briefly for the loop to process remaining items and exit cleanly
                    await asyncio.wait_for(self._main_loop_task, timeout=max(1.0, self._queue_timeout * 5))
                except asyncio.TimeoutError:
                    logger.warning("Live loop task 未在超时内正常结束，将尝试取消。")
                    self._main_loop_task.cancel()
                    try:
                        await self._main_loop_task # Await cancellation
                    except asyncio.CancelledError:
                        logger.info("Live loop task 已成功取消。")
                except Exception as e:
                    logger.error(f"等待 live loop task 结束时出错: {e}", exc_info=True)
                if self._main_loop_task and not self._main_loop_task.done():
                     logger.error("Live loop task 停止失败。")
                else:
                     logger.info("Live loop task 已结束。")
            else:
                 logger.info(f"Async 事件引擎 '{self._name}' (backtest 模式) 已标记为停止状态。")
            # Optionally cancel/await other pending handler tasks if tracked? More complex.
        else:
             logger.warning(f"Async 事件引擎 '{self._name}' 尚未启动或已停止。")


    def qsize(self) -> int:
         """返回事件队列的当前大小"""
         return self._queue.qsize()