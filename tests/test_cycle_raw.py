from pathlib import Path

from mermaid_timeline.cycle_raw import iter_cycle_events
from mermaid_timeline.models import LogEventType


def test_iter_cycle_events_infers_basic_types(tmp_path: Path) -> None:
    path = tmp_path / "sample.CYCLE.h"
    path.write_text("INFO boot\nwarning soon\nplain message\n", encoding="utf-8")

    events = list(iter_cycle_events(path))

    assert len(events) == 3
    assert events[0].event_type is LogEventType.INFO
    assert events[1].event_type is LogEventType.WARNING
    assert events[2].event_type is LogEventType.UNKNOWN
