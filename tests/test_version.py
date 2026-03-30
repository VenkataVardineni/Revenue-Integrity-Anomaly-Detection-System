"""Package version is defined and non-empty."""

from _version import __version__


def test_version_is_semverish():
    assert __version__
    parts = __version__.split(".")
    assert len(parts) >= 2
    assert all(p.isdigit() for p in parts[:2])
