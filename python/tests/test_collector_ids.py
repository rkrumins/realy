"""The collector's first-decision dedup handles every id layout the generator produces."""
from realy.collect.collector import SeenIds


def test_unified_and_part_ids_are_tracked_densely():
    seen = SeenIds()
    gen = 3
    unified = [(gen << 48) | s for s in (0, 1, 70_000)]
    parts = [(gen << 48) | (1 << 47) | s for s in (0, 5, 123_456)]   # a re-scored stage's id
    for eid in unified + parts:
        assert seen.add(eid)
    for eid in unified + parts:
        assert not seen.add(eid)                                      # a duplicate
    assert seen.add((gen << 48) | 2)                                  # its own range, unaffected
    # memory follows the ids sent, not their magnitude (bit 47 would need 2^47 slots)
    assert seen.nbytes() < 2_000_000


def test_generators_do_not_collide():
    seen = SeenIds()
    assert seen.add((1 << 48) | 7)
    assert seen.add((2 << 48) | 7)
    assert seen.add((1 << 48) | (1 << 47) | 7)
    assert not seen.add((2 << 48) | 7)
