# components/data_feed.py
import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any

from components.base import BaseComponent
from core.event_bus import EventBus
from core.events import MarketEvent, BacktestStartEvent, BacktestEndEvent

logger = logging.getLogger(__name__)

class BaseDataFeed(BaseComponent):
    """
    Base class for Data Feed components.
    Responsible for providing market data.
    """
    def __init__(self, event_bus: EventBus):
        super().__init__(event_bus)
        self._running = False # State to control the data feed loop

    async def start_feed(self):
        """
        Starts the data feed process.
        This method should be overridden by subclasses.
        """
        logger.info("BaseDataFeed started. Override start_feed in subclass.")
        self._running = True
        # Subclasses will implement the loop to fetch/publish data

    async def stop_feed(self):
        """
        Stops the data feed process gracefully.
        This method should be overridden by subclasses.
        """
        logger.info("BaseDataFeed stopping. Override stop_feed in subclass.")
        self._running = False
        # Subclasses will implement logic to stop their data source (e.g., close websocket)


class MockDataFeed(BaseDataFeed):
    """
    A simple mock data feed for testing and basic backtesting simulation.
    Publishes a predefined sequence of MarketEvents.
    """
    def __init__(self, event_bus: EventBus, market_data: List[Dict[str, Any]]):
        """
        Args:
            event_bus: The central Event Bus instance.
            market_data: A list of dictionaries, each representing a data point.
                         Each dict MUST have a 'timestamp' (datetime) and 'symbol' key.
                         Example: [{'timestamp': ..., 'symbol': 'AAPL', 'close': 150.0}, ...]
        """
        super().__init__(event_bus)
        # Sort data by timestamp to simulate time progression
        self.market_data = sorted(market_data, key=lambda x: x['timestamp'])
        self._current_index = 0
        self._is_backtesting = False # Assume mock is primarily for backtest simulation

    def _setup_event_handlers(self):
        """MockDataFeed subscribes to BacktestStart/End events."""
        self.event_bus.subscribe(BacktestStartEvent, self._handle_backtest_start)
        self.event_bus.subscribe(BacktestEndEvent, self._handle_backtest_end)

    async def _handle_backtest_start(self, event: BacktestStartEvent):
        """Handle backtest start event."""
        logger.info(f"MockDataFeed received BacktestStartEvent. Preparing data between {event.start_time} and {event.end_time}")
        self._is_backtesting = True
        self._current_index = 0
        # Filter data based on backtest range if needed
        # For simplicity, we'll just use the provided sorted data

    async def _handle_backtest_end(self, event: BacktestEndEvent):
        """Handle backtest end event."""
        logger.info("MockDataFeed received BacktestEndEvent. Stopping data feed loop.")
        self._running = False # Signal the run loop to stop

    async def start_feed(self):
        """
        Starts the mock data feed, publishing events step-by-step
        and waiting for the bus to clear between steps (like BacktestDataFeed).
        """
        logger.info("MockDataFeed started.")
        self._running = True

        # Wait for BacktestStartEvent before publishing data if running in backtest mode
        # In a real setup, the main runner would ensure BacktestStart is published BEFORE calling start_feed
        # For this mock, we assume it's called after. Or we could have a state check.
        # Let's assume BacktestStartEvent is published and we react to it via handler setup.

        while self._running and self._current_index < len(self.market_data):
            data_point = self.market_data[self._current_index]
            timestamp = data_point.get('timestamp') # Assuming 'timestamp' exists

            if not timestamp:
                logger.warning(f"Skipping data point at index {self._current_index} due to missing timestamp.")
                self._current_index += 1
                continue

            # Create and publish MarketEvent
            # Ensure timestamp is timezone-aware if needed, or standardize to UTC
            if timestamp.tzinfo is None:
                 # Assume naive timestamps are UTC for simplicity, or handle timezone properly
                 timestamp = timestamp.replace(tzinfo=timezone.utc)
            else:
                 timestamp = timestamp.astimezone(timezone.utc)


            event_data = {k: v for k, v in data_point.items() if k not in ['timestamp', 'symbol']}
            market_event = MarketEvent(
                symbol=data_point['symbol'],
                timestamp=timestamp,
                data=event_data
            )

            logger.info(f"MockDataFeed publishing MarketEvent for {market_event.symbol} at {market_event.timestamp}")
            self.event_bus.publish(market_event)

            # --- Crucial Backtest Sync Point ---
            # Wait for the event bus to process events triggered by the last MarketEvent
            # This simulates the time step in backtesting.
            await self.event_bus.wait_until_queue_empty()
            logger.debug(f"MockDataFeed finished waiting for queue after publishing {market_event.symbol} at {market_event.timestamp}")

            self._current_index += 1

        # After loop finishes (data exhausted or stopped), publish BacktestEndEvent if in backtest mode
        if self._is_backtesting:
             logger.info("MockDataFeed finished publishing all historical data.")
             # Publish BacktestEndEvent if it wasn't already handled
             # In a real setup, the main runner might handle this, but for self-contained mock
             if self._running: # Only publish if not already stopped by external BacktestEnd
                  self.event_bus.publish(BacktestEndEvent(end_time=datetime.utcnow().replace(tzinfo=timezone.utc)))


        self._running = False
        logger.info("MockDataFeed stopped.")


    async def stop_feed(self):
        """Stops the mock data feed."""
        logger.info("MockDataFeed stopping requested.")
        self._running = False


class LiveDataFeed(BaseDataFeed): # Inherit from BaseDataFeed
    """
    A data feed for live trading.
    Connects to a real-time data source (e.g., WebSocket) and publishes MarketEvents.
    """
    def __init__(self, event_bus: EventBus, api_config: Dict[str, Any]):
        """
        Args:
            event_bus: The central Event Bus instance.
            api_config: Dictionary containing configuration for the data API (e.g., URL, API keys).
        """
        super().__init__(event_bus)
        self.api_config = api_config
        self._connection = None # Placeholder for API connection object
        self._symbols_to_subscribe: List[str] = [] # List of symbols to subscribe to

        logger.info(f"{self.__class__.__name__} initialized.")

    # No specific event handlers setup needed by default, it just publishes data

    async def start_feed(self):
        """
        Establishes connection to the live data source and starts listening.
        This method should be awaited.
        """
        if self._running:
            logger.warning("LiveDataFeed is already running.")
            return

        logger.info("LiveDataFeed attempting to connect to live data source...")
        self._running = True

        try:
            # --- Real API Connection Logic Goes Here ---
            # Example using websockets:
            # self._connection = await websockets.connect(self.api_config['websocket_url'])
            # logger.info("LiveDataFeed connected.")

            # Example using a mock async generator for testing without a real API
            async for market_data_point in self._mock_realtime_data():
                if not self._running:
                    break # Stop if stop_feed was called

                # Process the raw data point and publish MarketEvent
                await self._process_raw_data(market_data_point)

            # --- End of Real API Connection Logic ---

        except asyncio.CancelledError:
            logger.info("LiveDataFeed start loop cancelled.")
        except Exception as e:
            logger.exception(f"Error in LiveDataFeed: {e}")
        finally:
            await self.stop_feed() # Ensure stop_feed is called on exit


    async def stop_feed(self):
        """
        Closes the connection to the live data source gracefully.
        This method should be awaited.
        """
        if not self._running:
            logger.warning("LiveDataFeed is not running.")
            return

        logger.info("LiveDataFeed stopping. Closing connection...")
        self._running = False

        # --- Real API Disconnection Logic Goes Here ---
        # Example:
        # if self._connection:
        #     await self._connection.close()
        #     logger.info("LiveDataFeed connection closed.")
        # self._connection = None
        # --- End of Real API Disconnection Logic ---

        logger.info("LiveDataFeed stopped.")


    async def subscribe_symbols(self, symbols: List[str]):
        """
        Sends subscription request to the data source for the given symbols.
        This method should be awaited as API calls are often asynchronous.
        """
        self._symbols_to_subscribe = symbols
        logger.info(f"LiveDataFeed subscribing to symbols: {symbols}")
        # --- Real API Subscription Logic Goes Here ---
        # Example: Send a message over the websocket connection
        # if self._connection:
        #     subscribe_message = {"type": "subscribe", "symbols": symbols} # Example format
        #     await self._connection.send(json.dumps(subscribe_message))
        # --- End of Real API Subscription Logic ---
        # In a real scenario, you'd handle confirmation of subscription

    async def _process_raw_data(self, raw_data: Dict[str, Any]):
        """
        Internal method to process raw data received from the API and publish MarketEvents.
        This method is called by the data listening loop in start_feed.
        """
        # --- Data Transformation Logic Goes Here ---
        # Example: Assuming raw_data is like {'type': 'bar', 'symbol': 'AAPL', 'open': ..., 'timestamp': ...}
        # Need to parse raw_data based on the specific API format
        try:
            event_type = raw_data.get('type')
            symbol = raw_data.get('symbol')
            timestamp_raw = raw_data.get('timestamp')
            data = {k: v for k, v in raw_data.items() if k not in ['type', 'symbol', 'timestamp']} # Extract relevant data

            if event_type == 'bar' and symbol and timestamp_raw:
                 # Convert timestamp to datetime, ensure timezone-aware (UTC recommended)
                 # Example: If timestamp_raw is a Unix epoch timestamp (seconds)
                 # timestamp = datetime.fromtimestamp(timestamp_raw, tz=timezone.utc)
                 # Example: If timestamp_raw is ISO 8601 string
                 # timestamp = datetime.fromisoformat(timestamp_raw.replace('Z', '+00:00')).astimezone(timezone.utc)
                 # For mock, let's just use a placeholder
                 timestamp = datetime.utcnow().replace(tzinfo=timezone.utc) # Placeholder

                 market_event = MarketEvent(
                     symbol=symbol,
                     timestamp=timestamp,
                     data=data # Pass parsed data
                 )
                 logger.debug(f"LiveDataFeed publishing MarketEvent for {market_event.symbol} at {market_event.timestamp}")
                 self.event_bus.publish(market_event)
            # Add handling for other event types like 'tick', 'trade', etc.

        except Exception as e:
            logger.exception(f"Error processing raw data: {raw_data}. Error: {e}")

        # --- End of Data Transformation Logic ---


    async def _mock_realtime_data(self):
        """
        A mock async generator to simulate receiving data in real-time.
        Replace this with your actual API data receiving loop.
        """
        logger.info("Using mock realtime data feed (no real API connection).")
        mock_data_points = [
            {'type': 'bar', 'symbol': 'AAPL', 'open': 160.0, 'high': 161.0, 'low': 159.5, 'close': 160.5, 'volume': 50000, 'timestamp': datetime.utcnow().timestamp()},
            {'type': 'bar', 'symbol': 'MSFT', 'open': 270.0, 'high': 271.0, 'low': 269.5, 'close': 270.8, 'volume': 30000, 'timestamp': datetime.utcnow().timestamp() + 1},
            {'type': 'bar', 'symbol': 'AAPL', 'open': 160.5, 'high': 161.5, 'low': 160.0, 'close': 161.2, 'volume': 60000, 'timestamp': datetime.utcnow().timestamp() + 60}, # Next minute
            {'type': 'bar', 'symbol': 'MSFT', 'open': 270.8, 'high': 271.8, 'low': 270.2, 'close': 271.5, 'volume': 40000, 'timestamp': datetime.utcnow().timestamp() + 61}, # Next minute
            # ... add more mock data
        ]
        for data_point in mock_data_points:
            if not self._running:
                break
            # Simulate delay between receiving data points
            await asyncio.sleep(0.5) # Simulate data arriving every 0.5 seconds
            yield data_point

        logger.info("Mock realtime data feed exhausted.")
        self._running = False # Signal end after mock data runs out