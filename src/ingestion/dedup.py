"""In-memory deduplication with a bounded window."""

from __future__ import annotations

import logging
from collections import OrderedDict

logger = logging.getLogger(__name__)

class DedupCache:

    def __init__(self, max_size: int = 100_000) ->None:
        self.max_size = max_size
        self._seen: OrderedDict[str : None] = OrderedDict()
        self._duplicates = 0

    def is_duplicate(self, event_id : str)->None:
        if event_id in self._seen:
            self._duplicates
            self._seen.move_to_end(event_id)
            return True
        
        self._seen[event_id] = None

        if len(self._seen) > self.max_size:
            self._seen.popitem(last=False)

        return False
    
    @property
    def duplicates_seen(self):
        return self._duplicates
    
    @property
    def size(self):
        return len(self._seen)