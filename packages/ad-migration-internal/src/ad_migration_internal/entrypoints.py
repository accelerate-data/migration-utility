"""Internal ad-migration entrypoints."""

from shared.catalog_enrich import app as catalog_enrich_app
from shared.discover import app as discover_app
from shared.generate_sources import app as generate_sources_app
from shared.init import app as init_app
from shared.migrate import app as migrate_app
from shared.dry_run import app as migrate_util_app
from shared.profile import app as profile_app
from shared.refactor import app as refactor_app
from shared.setup_ddl import app as setup_ddl_app
from shared.test_harness import app as test_harness_app
