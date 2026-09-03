"""History persistence, and specifically what happens to readings that did not advance
current state.

The interesting case is the reconnect backlog: the broker delivers everything it queued,
only the newest reading advances the live view, and every earlier one is dropped by the
seq guard. If history followed the live view it would be empty for exactly the window
somebody wanted to inspect.
"""

from conftest import telemetry

from app.history import HistoryWriter
from app.ingest import handle_message

TOPIC = "fleet/robots/r1/telemetry"


def writer(tmp_path):
    return HistoryWriter(str(tmp_path / "history.db"), flush_interval=999).open()


def wire(**kwargs):
    return telemetry(**kwargs).model_dump_json().encode()


def test_accepted_readings_are_recorded(fleet, tmp_path):
    store = writer(tmp_path)
    handle_message(fleet, TOPIC, wire(seq=1, x=10.0), store)
    store.flush()

    points = store.query("r1")
    assert [p["seq"] for p in points] == [1]
    assert points[0]["x"] == 10.0


def test_a_late_reading_is_kept_even_though_it_did_not_advance_state(fleet, tmp_path):
    store = writer(tmp_path)

    handle_message(fleet, TOPIC, wire(seq=100, ts=2000.0, x=100.0), store)
    # A straggler from before the disruption, arriving after a newer reading already
    # landed. It must not move the live view backwards...
    changed = handle_message(fleet, TOPIC, wire(seq=50, ts=1000.0, x=50.0), store)
    store.flush()

    assert changed == []
    assert fleet.get("r1").x == 100.0
    # ...but the point itself is real data and belongs in the history.
    assert {p["seq"] for p in store.query("r1")} == {50, 100}


def test_redelivery_does_not_duplicate_a_history_row(fleet, tmp_path):
    store = writer(tmp_path)

    payload = wire(seq=7)
    handle_message(fleet, TOPIC, payload, store)
    handle_message(fleet, TOPIC, payload, store)  # QoS 1 redelivery
    store.flush()

    # The (robot_id, seq) primary key is what makes recording every valid reading safe.
    assert len(store.query("r1")) == 1


def test_malformed_payload_is_not_recorded(fleet, tmp_path):
    store = writer(tmp_path)
    handle_message(fleet, TOPIC, b"not json", store)
    store.flush()

    assert store.query("r1") == []


def test_query_filters_by_time_range(fleet, tmp_path):
    store = writer(tmp_path)
    for seq, ts in [(1, 1000.0), (2, 2000.0), (3, 3000.0)]:
        handle_message(fleet, TOPIC, wire(seq=seq, ts=ts), store)
    store.flush()

    assert {p["seq"] for p in store.query("r1", t_from=1500.0, t_to=2500.0)} == {2}
    assert len(store.query("r1", limit=2)) == 2


def test_writes_are_buffered_until_flushed(fleet, tmp_path):
    # Disk I/O stays off the ingest path; nothing is committed per message.
    store = writer(tmp_path)
    handle_message(fleet, TOPIC, wire(seq=1), store)

    assert store.query("r1") == []
    assert store.flush() == 1
    assert len(store.query("r1")) == 1


def test_close_flushes_what_is_buffered(fleet, tmp_path):
    path = tmp_path / "history.db"
    store = HistoryWriter(str(path), flush_interval=999).open()
    handle_message(fleet, TOPIC, wire(seq=1), store)
    store.close()

    reopened = HistoryWriter(str(path), flush_interval=999).open()
    assert len(reopened.query("r1")) == 1
