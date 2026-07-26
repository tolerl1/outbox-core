from __future__ import annotations

import importlib.resources

import pytest

_SKILL_NAMES = ("outbox-integrate", "outbox-new-provider", "outbox-review")


@pytest.mark.parametrize("skill_name", _SKILL_NAMES)
def test_skill_is_bundled_into_the_installed_package(skill_name: str) -> None:
    """Verify each consumer-facing skill ships inside the outbox package.

    Skills live at `outbox/.agents/skills/<name>/SKILL.md` (the
    tiangolo/library-skills convention), not in a top-level `skills/`
    directory, so they're importable via `importlib.resources` from a pip
    install and discoverable by the `library-skills` tool.
    """
    skill_md = (
        importlib.resources.files("outbox")
        .joinpath(".agents", "skills", skill_name, "SKILL.md")
        .read_text(encoding="utf-8")
    )

    assert skill_md.startswith("---\n")
    assert f"name: {skill_name}\n" in skill_md
