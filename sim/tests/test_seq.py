"""next_seq(): the token the backend orders and deduplicates on.

Worth its own file because every failure mode here is silent. A token that repeats or
goes backwards does not raise -- the backend's guard simply drops the reading, and a
robot quietly stops updating on the operator's screen.
"""

import robot
from robot import next_seq


def reset(value=0):
    robot._last_seq = value


def test_advances_even_within_the_same_millisecond():
    reset()
    same_instant = 1_700_000_000.0

    tokens = [next_seq(same_instant) for _ in range(5)]

    assert tokens == sorted(set(tokens))  # strictly increasing, no repeats


def test_tracks_the_clock_when_time_actually_passes():
    reset()
    first = next_seq(1_700_000_000.0)
    later = next_seq(1_700_000_060.0)

    assert later - first == 60_000  # a minute of wall clock, in milliseconds


def test_survives_a_restart_without_resuming_at_zero():
    # What a crashed robot looks like: fresh process, counter back to 0, clock unchanged.
    reset()
    before_crash = next_seq(1_700_000_000.0)

    reset()  # the restart
    after_restart = next_seq(1_700_000_005.0)

    # Must be ahead of where it left off, or the backend rejects everything it sends.
    assert after_restart > before_crash


def test_never_goes_backwards_when_the_clock_steps_back():
    reset()
    before = next_seq(1_700_000_060.0)
    after = next_seq(1_700_000_000.0)  # NTP correction, clock jumps back a minute

    assert after > before
