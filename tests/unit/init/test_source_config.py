from __future__ import annotations

import pytest

from shared.init_support.source_config import SOURCE_REGISTRY, SourceConfig, get_source_config


def test_get_source_config_returns_registered_config() -> None:
    config = get_source_config("sql_server")

    assert isinstance(config, SourceConfig)
    assert config.slug == "sql_server"
    assert config.display_name == "SQL Server"
    assert set(SOURCE_REGISTRY) == {"sql_server", "oracle"}


def test_get_source_config_rejects_unknown_technology() -> None:
    with pytest.raises(ValueError, match="Unknown technology"):
        get_source_config("postgres")
