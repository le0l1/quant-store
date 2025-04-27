# event_engine.py
import queue
import threading
import time
from typing import Callable, Dict, List, Optional
from collections import defaultdict
import logging

# 从 event.py 导入事件基类
from event import Event

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EventEngine:
    """
    事件驱动引擎核心。
    负责事件的分发和处理。
    """
    def __init__(self, name: str = "DefaultEngine", queue_timeout: float = 0.1):
        """
        初始化事件引擎。
        :param name: 引擎名称，用于日志区分。
        :param queue_timeout: 从队列获取事件的超时时间（秒），防止永久阻塞。
        """
        self._name = name
        self._queue = queue.Queue()
        self._active = False # 控制事件循环是否运行
        self._thread = threading.Thread(target=self._run, name=f"{name}Thread")
        # 使用 defaultdict(list) 简化处理器管理
        self._handlers: Dict[str, List[Callable[[Event], None]]] = defaultdict(list)
        self._general_handlers: List[Callable[[Event], None]] = [] # 处理所有事件类型的处理器
        self._queue_timeout = queue_timeout
        logger.info(f"事件引擎 '{self._name}' 初始化完成。")

    def _run(self):
        """
        主事件循环。
        """
        logger.info(f"事件引擎 '{self._name}' 线程启动。")
        while self._active:
            try:
                # 从队列获取事件，带有超时
                event = self._queue.get(block=True, timeout=self._queue_timeout)
                # logger.debug(f"引擎 '{self._name}' 接收到事件: {event}") # 可以取消注释以调试

                # 分发给特定事件类型的处理器
                if event.event_type in self._handlers:
                    specific_handlers = self._handlers[event.event_type][:] # 复制列表以防处理中修改
                    for handler in specific_handlers:
                        try:
                            # logger.debug(f"调用处理器 {handler.__name__} 处理 {event.event_type}")
                            handler(event)
                        except Exception as e:
                            logger.error(f"处理器 {getattr(handler, '__name__', 'unknown')} 处理事件 {event} 时出错: {e}", exc_info=True)

                # 分发给通用处理器
                general_handlers_copy = self._general_handlers[:] # 复制列表
                for handler in general_handlers_copy:
                     try:
                         # logger.debug(f"调用通用处理器 {handler.__name__} 处理 {event.event_type}")
                         handler(event)
                     except Exception as e:
                         logger.error(f"通用处理器 {getattr(handler, '__name__', 'unknown')} 处理事件 {event} 时出错: {e}", exc_info=True)

                # 标记任务完成 (如果使用 JoinableQueue 可能需要)
                # self._queue.task_done()

            except queue.Empty:
                # 队列为空时，短暂暂停，然后继续检查 _active 状态
                # time.sleep(0.01) # 可以加一个小延迟，但 timeout 机制通常足够
                continue
            except Exception as e:
                logger.error(f"事件引擎 '{self._name}' 运行中发生未预期错误: {e}", exc_info=True)
                # 考虑是否需要停止引擎或采取其他措施
                # self.stop() # 例如，遇到严重错误时停止

        logger.info(f"事件引擎 '{self._name}' 线程正常停止。")

    def register(self, event_type: Optional[str], handler: Callable[[Event], None]):
        """
        注册事件处理器。
        :param event_type: 要监听的事件类型字符串。如果为 None，则注册为通用处理器。
        :param handler: 事件处理函数或方法。
        """
        if not callable(handler):
             logger.error(f"注册失败：提供的处理器 {handler} 不是可调用对象。")
             return

        if event_type is None:
            if handler not in self._general_handlers:
                self._general_handlers.append(handler)
                logger.info(f"通用处理器 {getattr(handler, '__name__', 'unknown')} 已注册。")
            else:
                 logger.warning(f"通用处理器 {getattr(handler, '__name__', 'unknown')} 已存在，未重复注册。")
        else:
            if handler not in self._handlers[event_type]:
                self._handlers[event_type].append(handler)
                logger.info(f"处理器 {getattr(handler, '__name__', 'unknown')} 已注册监听事件类型 '{event_type}'。")
            else:
                logger.warning(f"处理器 {getattr(handler, '__name__', 'unknown')} 已注册监听事件类型 '{event_type}'，未重复注册。")

    def unregister(self, event_type: Optional[str], handler: Callable[[Event], None]):
        """
        注销事件处理器。
        :param event_type: 要取消监听的事件类型字符串。如果为 None，则从通用处理器注销。
        :param handler: 要注销的事件处理函数或方法。
        """
        if event_type is None:
            if handler in self._general_handlers:
                self._general_handlers.remove(handler)
                logger.info(f"通用处理器 {getattr(handler, '__name__', 'unknown')} 已注销。")
            else:
                logger.warning(f"尝试注销未注册的通用处理器 {getattr(handler, '__name__', 'unknown')}。")
        else:
            if event_type in self._handlers and handler in self._handlers[event_type]:
                self._handlers[event_type].remove(handler)
                logger.info(f"处理器 {getattr(handler, '__name__', 'unknown')} 已从事件类型 '{event_type}' 注销。")
                # 如果该类型没有处理器了，可以从字典中移除键（可选）
                if not self._handlers[event_type]:
                    del self._handlers[event_type]
            else:
                 logger.warning(f"尝试注销未注册的处理器 {getattr(handler, '__name__', 'unknown')} 或不存在的事件类型 '{event_type}'。")

    def put(self, event: Event):
        """
        将事件放入队列。
        :param event: 要放入队列的事件对象。
        """
        if not isinstance(event, Event):
            logger.error(f"尝试放入非 Event 类型的对象到队列: {event}")
            return
        # logger.debug(f"引擎 '{self._name}' 放入事件: {event}")
        self._queue.put(event)

    def start(self):
        """
        启动事件引擎的处理线程。
        """
        if not self._active:
            self._active = True
            self._thread.start()
            logger.info(f"事件引擎 '{self._name}' 已启动。")
        else:
            logger.warning(f"事件引擎 '{self._name}' 已经启动。")

    def stop(self, wait: bool = True, timeout: Optional[float] = None):
        """
        停止事件引擎的处理线程。
        :param wait: 是否等待线程完全结束。
        :param timeout: 等待线程结束的超时时间（秒）。如果为 None，则无限等待（如果 wait=True）。
        """
        if self._active:
            self._active = False
            if wait:
                logger.info(f"正在等待事件引擎 '{self._name}' 线程停止...")
                self._thread.join(timeout=timeout)
                if self._thread.is_alive():
                    logger.warning(f"事件引擎 '{self._name}' 线程在超时 ({timeout}s) 后仍未停止。")
                else:
                     logger.info(f"事件引擎 '{self._name}' 线程已成功停止。")
            else:
                 logger.info(f"已发送停止信号给事件引擎 '{self._name}' 线程，但不等待其结束。")
        else:
            logger.warning(f"事件引擎 '{self._name}' 尚未启动或已停止。")

    def is_active(self) -> bool:
        """检查引擎是否正在运行"""
        return self._active and self._thread.is_alive()

    def qsize(self) -> int:
        """返回事件队列的当前大小"""
        return self._queue.qsize()