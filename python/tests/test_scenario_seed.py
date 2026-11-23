"""The console's writable scenario directory follows the bundled scenarios without losing edits."""
from __future__ import annotations

from realy.control import scenario_seed as ss


def _dirs(tmp_path):
    b, d = tmp_path / "bundled", tmp_path / "data"
    b.mkdir()
    (b / "smoke.yaml").write_text("name: smoke\nv: 1\n")
    (b / "flows.yaml").write_text("name: flows\nv: 1\n")
    return b, d


def test_unedited_copies_follow_the_image_and_edited_ones_are_kept(tmp_path):
    b, d = _dirs(tmp_path)
    assert sorted(ss.seed(b, d, log=lambda m: None)["added"]) == ["flows.yaml", "smoke.yaml"]
    (d / "smoke.yaml").write_text("name: smoke\nv: 1\nmine: true\n")       # edited by hand
    (b / "smoke.yaml").write_text("name: smoke\nv: 2\n")                   # the image moves on
    (b / "flows.yaml").write_text("name: flows\nv: 2\n")
    (d / "mine.yaml").write_text("name: mine\n")                          # the user's own scenario
    out = ss.seed(b, d, log=lambda m: None)
    assert out["updated"] == ["flows.yaml"] and out["kept_edited"] == ["smoke.yaml"]
    assert "v: 2" in (d / "flows.yaml").read_text() and "mine: true" in (d / "smoke.yaml").read_text()
    assert (d / "mine.yaml").read_text() == "name: mine\n"
    st = ss.status(b, d)
    assert st["smoke"] == {"bundled": True, "edited": True, "update_available": True}
    assert st["flows"] == {"bundled": True, "edited": False, "update_available": False}
    # reset: the bundled version back, the edit kept aside
    assert ss.reset(b, d, "smoke") and "v: 2" in (d / "smoke.yaml").read_text()
    assert any("mine: true" in p.read_text() for p in (d / ".backup").glob("smoke.*.yaml"))
    assert ss.status(b, d)["smoke"]["edited"] is False
    assert not ss.reset(b, d, "mine")                                     # not bundled


def test_a_directory_seeded_before_the_record_is_migrated_without_losing_anything(tmp_path):
    b, d = _dirs(tmp_path)
    d.mkdir()
    (d / "smoke.yaml").write_text("name: smoke\nv: 0\n")                   # an old bundled version (or an edit)
    (d / "flows.yaml").write_text("name: flows\nv: 1\n")                   # identical to the bundled one
    out = ss.seed(b, d, log=lambda m: None)
    assert out["backed_up"] == ["smoke.yaml"] and out["unchanged"] == ["flows.yaml"]
    assert "v: 1" in (d / "smoke.yaml").read_text()
    assert [p.read_text() for p in (d / ".backup").glob("smoke.*.yaml")] == ["name: smoke\nv: 0\n"]
    assert not list(d.glob(".backup/*.yaml"))[0].match("*/data/*.yaml")   # outside the scenario glob


def test_saving_from_the_console_marks_the_copy_edited(tmp_path):
    b, d = _dirs(tmp_path)
    ss.seed(b, d, log=lambda m: None)
    ss.mark_edited(d, "flows")
    (b / "flows.yaml").write_text("name: flows\nv: 3\n")
    assert ss.seed(b, d, log=lambda m: None)["kept_edited"] == ["flows.yaml"]
    assert "v: 1" in (d / "flows.yaml").read_text()
