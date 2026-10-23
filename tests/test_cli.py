import pytest

from flowcast import __version__
from flowcast.cli import main


def test_version_prints_the_package_version(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["version"]) == 0
    assert capsys.readouterr().out == f"flowcast {__version__}\n"


def test_a_missing_command_is_a_usage_error() -> None:
    with pytest.raises(SystemExit) as exc:
        main([])
    assert exc.value.code == 2
