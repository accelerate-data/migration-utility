"""Internal ad-migration entrypoints."""

from shared.catalog_enrich import app as catalog_enrich_app  # noqa: F401
from shared.discover import app as discover_app  # noqa: F401
from shared.generate_sources import app as generate_sources_app  # noqa: F401
from shared.init import app as init_app  # noqa: F401
from shared.migrate import app as migrate_app  # noqa: F401
from shared.dry_run import app as migrate_util_app  # noqa: F401
from shared.profile import app as profile_app  # noqa: F401
from shared.refactor import app as refactor_app  # noqa: F401
from shared.setup_ddl import app as setup_ddl_app  # noqa: F401
from shared.test_harness import app as test_harness_app  # noqa: F401
