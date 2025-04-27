# data_source.py (Refined for Simplicity)
import asyncio
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime
import logging
from typing import List, Optional, Any
# Assuming async engine and specific events are imported
from trader.base.event_engine import EventEngine
from trader.base.event import MarketEvent, SystemEvent

logger = logging.getLogger(__name__)

# --- Refined IDataSource Interface ---
class IDataSource(ABC):
    """
    数据源接口 (抽象基类) - Refined for Simplicity。
    Base class handles start/stop and task management.
    Subclasses primarily implement _run_generator_loop.
    """
    def __init__(self, event_engine: EventEngine, symbols: List[str],
                 start_date: Optional[datetime] = None,
                 end_date: Optional[datetime] = None):
        """
        Standardized initialization.
        """
        self.event_engine = event_engine
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        self._active = False
        self._generator_task: Optional[asyncio.Task] = None
        logger.info(f"{self.__class__.__name__} initialized for symbols: {self.symbols}.")

    async def start(self):
        """
        Start the data source by creating and running the generator loop task.
        """
        if self._active:
            logger.warning(f"{self.__class__.__name__} is already active.")
            return
        logger.info(f"Starting {self.__class__.__name__}...")
        self._active = True
        # Create and schedule the main loop task
        self._generator_task = asyncio.create_task(self._run_generator_loop())
        await self._generator_task # Wait for task completion
        logger.info(f"{self.__class__.__name__} generator task created.")

    async def stop(self):
        """
        Stop the data source by cancelling the generator loop task.
        """
        if not self._active:
            logger.warning(f"{self.__class__.__name__} is not active.")
            return
        logger.info(f"Stopping {self.__class__.__name__}...")
        self._active = False # Signal loop to stop (important!)
        if self._generator_task and not self._generator_task.done():
            self._generator_task.cancel()
            try:
                await self._generator_task # Wait for cancellation
            except asyncio.CancelledError:
                logger.info(f"{self.__class__.__name__} generator task cancelled.")
            except Exception as e:
                logger.error(f"Error during {self.__class__.__name__} task cancellation: {e}", exc_info=True)
        await self._cleanup() # Perform any subclass cleanup
        logger.info(f"{self.__class__.__name__} stopped.")

    @abstractmethod
    async def _run_generator_loop(self):
        """
        【Subclass Must Implement】The core loop for fetching/reading data
        and putting MarketEvents onto the event engine's queue.
        This loop should check `self._active` periodically and exit if False.
        It must put a final 'DATASOURCE_COMPLETE' SystemEvent before exiting normally.
        For backtesting, it should await engine.run_sync_cycle_async() after putting events.
        """
        pass

    async def _cleanup(self):
        """
        【Subclass Optional Override】Cleanup resources on stop.
        """
        logger.debug(f"{self.__class__.__name__} performing default cleanup.")
        pass

    # Helper method for subclasses to signal completion consistently
    async def _signal_completion(self, last_timestamp: Optional[datetime] = None, error: bool = False):
        """Puts the completion event onto the queue."""
        logger.info(f"DataSource signaling completion (Error={error})...")
        completion_event = SystemEvent(
            timestamp=last_timestamp or datetime.now(),
            message="DATASOURCE_COMPLETE_ERROR" if error else "DATASOURCE_COMPLETE"
        )
        self.event_engine.put(completion_event)
        # Process this final event if in backtest mode
        if getattr(self.event_engine, '_mode', 'live') == 'backtest' and \
           hasattr(self.event_engine, 'run_sync_cycle_async'):
            await self.event_engine.run_sync_cycle_async()


