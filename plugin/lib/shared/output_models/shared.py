"""Shared configuration for output contract models."""

from pydantic import ConfigDict

OUTPUT_CONFIG = ConfigDict(extra="forbid")

