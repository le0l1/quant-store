from trader.base.datafeed import DataFeed
from trader.base.event_engine import EventEngine
from typing import List, Optional
from datetime import datetime
import pandas as pd
import asyncio
from trader.base.event import MarketEvent

import logging
logger = logging.getLogger(__name__)

class CSVDataFeed(DataFeed):
    def __init__(self, event_engine: EventEngine, symbols: List[str],
                 file_path: str,
                 start_date: Optional[datetime] = None,
                 end_date: Optional[datetime] = None,
                ): # Specific param
        super().__init__(event_engine, symbols, start_date, end_date)
        self.file_path = file_path
        self._dataframe: Optional[pd.DataFrame] = None # Store loaded data
        

    def _load_and_filter_data(self) -> pd.DataFrame:
        combined_data = pd.read_csv(self.file_path) # Load CSV
        combined_data['timestamp'] = pd.to_datetime(combined_data['timestamp']) # Convert to datetime
        combined_data.set_index('timestamp', inplace=True) # Set timestamp as index
        combined_data.sort_index(inplace=True) # Sort by timestamp

        if self.start_date:
             logger.debug(f"Applying start date filter: >= {self.start_date}")
             combined_data = combined_data[combined_data.index >= self.start_date] # Basic filter
        if self.end_date:
             logger.debug(f"Applying end date filter: <= {self.end_date}")
             combined_data = combined_data[combined_data.index <= self.end_date] # Basic filter

        logger.info(f"CSV data loaded and filtered, {len(combined_data)} records.")
        return combined_data.reset_index()

    async def connect(self):
        """Loads data and puts MarketEvents onto the queue sequentially."""
        logger.info("CSVDataSource: Generator loop started.")
        last_event_time = None
        is_backtest_mode = getattr(self.event_engine, '_mode', 'live') == 'backtest'

        logger.info(f"Backtest mode: {is_backtest_mode}") # Debug loggin

        try:
            self._dataframe = self._load_and_filter_data()

            if self._dataframe.empty:
                logger.warning("No data loaded after filtering. Signaling completion.")
                await self._signal_completion()
                return

            logger.info(f"Injecting {len(self._dataframe)} events...")
            for index, row in self._dataframe.iterrows():
                # Check if stop was requested between events
                if not self._active:
                    logger.info("Stop requested during event injection.")
                    break

                # --- Create and Put Event ---
                try:
                    ts = pd.to_datetime(row['timestamp'])
                    last_event_time = ts # Track last timestamp processed
                    event = MarketEvent(
                        timestamp=ts, symbol=str(row['symbol']),
                        open_price=float(row['open']), high_price=float(row['high']),
                        low_price=float(row['low']), close_price=float(row['close']),
                        volume=float(row['volume'])
                    )
                    self.event_engine.put(event)
                    await asyncio.sleep(0.01) 
                except Exception as e:
                     logger.error(f"Error creating/putting event for row {index}: {e}")
        except asyncio.CancelledError:
             logger.info("CSVDataSource generator loop cancelled.")
             # Don't signal completion if cancelled externally
        except Exception as e:
             logger.error(f"Error in CSVDataSource generator loop: {e}", exc_info=True)
             await self._signal_completion(last_event_time, error=True) # Signal error


    async def disconnect(self):
        """Clear the loaded dataframe."""
        logger.debug("Cleaning up CSVDataSource: Clearing DataFrame.")
        self._dataframe = None # Allow memory to be reclaimed