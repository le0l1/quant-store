import logging
from core.event_bus import EventBus

# 配置日志
logger = logging.getLogger(__name__)

class BaseComponent:
    """Base class for all framework components."""

    def __init__(self, event_bus: EventBus):
        """
        Initializes the component with a reference to the Event Bus.

        Args:
            event_bus: The central Event Bus instance.
        """
        self.event_bus = event_bus
        self._setup_event_handlers() # Call setup method to register handlers

    def _setup_event_handlers(self):
        """
        Register event handlers for this component with the Event Bus.
        This method should be overridden by subclasses.
        """
        # Example: self.event_bus.subscribe(SomeEvent, self.handle_some_event)
        pass # Subclasses must override this
