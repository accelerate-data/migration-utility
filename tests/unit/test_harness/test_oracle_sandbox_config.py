"""Oracle sandbox configuration tests."""

from __future__ import annotations

import pytest

from shared.sandbox.oracle import OracleSandbox


class TestOracleConfigModuleBoundary:
    def test_from_env_lives_in_config_module(self) -> None:
        assert (
            OracleSandbox.from_env.__func__.__module__
            == "shared.sandbox.oracle_config"
        )

    def test_from_env_remains_available_from_public_facade(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("ORACLE_SOURCE_PASSWORD", "source-secret")
        monkeypatch.setenv("ORACLE_SANDBOX_PASSWORD", "sandbox-secret")

        backend = OracleSandbox.from_env(
            {
                "runtime": {
                    "source": {
                        "technology": "oracle",
                        "dialect": "oracle",
                        "connection": {
                            "host": "source-host",
                            "port": "15210",
                            "service": "SRCPDB",
                            "user": "source_user",
                            "schema": "SH",
                            "password_env": "ORACLE_SOURCE_PASSWORD",
                        },
                    },
                    "sandbox": {
                        "technology": "oracle",
                        "dialect": "oracle",
                        "connection": {
                            "host": "sandbox-host",
                            "port": "15211",
                            "service": "FREE",
                            "user": "sys",
                            "password_env": "ORACLE_SANDBOX_PASSWORD",
                        },
                    },
                }
            }
        )

        assert isinstance(backend, OracleSandbox)
        assert backend.host == "sandbox-host"
        assert backend.port == "15211"
        assert backend.cdb_service == "FREE"
        assert backend.password == "sandbox-secret"
        assert backend.source_host == "source-host"
        assert backend.source_service == "SRCPDB"
        assert backend.source_password == "source-secret"
        assert backend.source_schema == "SH"


class TestOracleSandboxFromEnv:
    def test_raises_when_oracle_pwd_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ORACLE_SANDBOX_PASSWORD", raising=False)
        monkeypatch.setenv("ORACLE_SOURCE_PASSWORD", "source-secret")
        with pytest.raises(ValueError, match="runtime.sandbox.connection.password_env"):
            OracleSandbox.from_env(
                {
                    "runtime": {
                        "source": {
                            "technology": "oracle",
                            "dialect": "oracle",
                            "connection": {
                                "host": "localhost",
                                "port": "1521",
                                "service": "FREEPDB1",
                                "user": "sh",
                                "schema": "SH",
                                "password_env": "ORACLE_SOURCE_PASSWORD",
                            },
                        },
                        "sandbox": {
                            "technology": "oracle",
                            "dialect": "oracle",
                            "connection": {
                                "host": "localhost",
                                "port": "1521",
                                "service": "FREEPDB1",
                                "user": "sys",
                            },
                        },
                    }
                }
            )

    def test_raises_when_source_schema_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ORACLE_SOURCE_PASSWORD", "source-secret")
        monkeypatch.setenv("ORACLE_SANDBOX_PASSWORD", "secret")
        with pytest.raises(ValueError, match="runtime.source.connection.schema"):
            OracleSandbox.from_env(
                {
                    "runtime": {
                        "source": {
                            "technology": "oracle",
                            "dialect": "oracle",
                            "connection": {
                                "host": "localhost",
                                "port": "1521",
                                "service": "FREEPDB1",
                                "user": "sh",
                                "password_env": "ORACLE_SOURCE_PASSWORD",
                            },
                        },
                        "sandbox": {
                            "technology": "oracle",
                            "dialect": "oracle",
                            "connection": {
                                "host": "localhost",
                                "port": "1521",
                                "service": "FREEPDB1",
                                "user": "sys",
                                "password_env": "ORACLE_SANDBOX_PASSWORD",
                            },
                        },
                    }
                }
            )

    def test_uses_explicit_runtime_roles(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ORACLE_SOURCE_PASSWORD", "source-secret")
        monkeypatch.setenv("ORACLE_SANDBOX_PASSWORD", "secret")
        backend = OracleSandbox.from_env(
            {
                "runtime": {
                    "source": {
                        "technology": "oracle",
                        "dialect": "oracle",
                        "connection": {
                            "host": "localhost",
                            "port": "1521",
                            "service": "FREEPDB1",
                            "user": "sh",
                            "schema": "SH",
                            "password_env": "ORACLE_SOURCE_PASSWORD",
                        },
                    },
                    "sandbox": {
                        "technology": "oracle",
                        "dialect": "oracle",
                        "connection": {
                            "host": "localhost",
                            "port": "1521",
                            "service": "FREEPDB1",
                            "user": "sys",
                            "password_env": "ORACLE_SANDBOX_PASSWORD",
                        },
                    },
                }
            }
        )
        assert backend.source_schema == "SH"
        assert backend.cdb_service == "FREEPDB1"
        assert backend.admin_user == "sys"
        assert backend.source_user == "sh"

    def test_allows_distinct_source_and_sandbox_services(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("ORACLE_SOURCE_PASSWORD", "source-secret")
        monkeypatch.setenv("ORACLE_SANDBOX_PASSWORD", "secret")
        backend = OracleSandbox.from_env(
            {
                "runtime": {
                    "source": {
                        "technology": "oracle",
                        "dialect": "oracle",
                        "connection": {
                            "host": "localhost",
                            "port": "1521",
                            "service": "SRCPDB",
                            "user": "sh",
                            "schema": "SH",
                            "password_env": "ORACLE_SOURCE_PASSWORD",
                        },
                    },
                    "sandbox": {
                        "technology": "oracle",
                        "dialect": "oracle",
                        "connection": {
                            "host": "localhost",
                            "port": "1521",
                            "service": "SANDBOXPDB",
                            "user": "sys",
                            "password_env": "ORACLE_SANDBOX_PASSWORD",
                        },
                    },
                }
            }
        )
        assert backend.source_service == "SRCPDB"
        assert backend.cdb_service == "SANDBOXPDB"


class TestCdbServiceRename:
    """self.service is renamed to self.cdb_service."""

    def test_cdb_service_stored(self) -> None:
        backend = OracleSandbox(
            host="localhost",
            port="1521",
            cdb_service="FREE",
            password="pw",
            admin_user="sys",
            source_schema="SH",
            source_service="FREEPDB1",
        )
        assert backend.cdb_service == "FREE"

    def test_no_service_attribute(self) -> None:
        backend = OracleSandbox(
            host="localhost",
            port="1521",
            cdb_service="FREE",
            password="pw",
            admin_user="sys",
            source_schema="SH",
            source_service="FREEPDB1",
        )
        assert not hasattr(backend, "service")

    def test_from_env_stores_cdb_service(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ORACLE_SOURCE_PASSWORD", "source-secret")
        monkeypatch.setenv("ORACLE_SANDBOX_PASSWORD", "secret")
        backend = OracleSandbox.from_env(
            {
                "runtime": {
                    "source": {
                        "technology": "oracle",
                        "dialect": "oracle",
                        "connection": {
                            "host": "localhost",
                            "port": "1521",
                            "service": "FREEPDB1",
                            "user": "sh",
                            "schema": "SH",
                            "password_env": "ORACLE_SOURCE_PASSWORD",
                        },
                    },
                    "sandbox": {
                        "technology": "oracle",
                        "dialect": "oracle",
                        "connection": {
                            "host": "localhost",
                            "port": "1521",
                            "service": "FREE",
                            "user": "sys",
                            "password_env": "ORACLE_SANDBOX_PASSWORD",
                        },
                    },
                }
            }
        )
        assert backend.cdb_service == "FREE"
        assert not hasattr(backend, "service")
