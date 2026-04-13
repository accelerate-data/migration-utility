"""catalog-enrich output contracts."""

from pydantic import BaseModel

from shared.output_models.shared import OUTPUT_CONFIG


class CatalogEnrichOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    tables_augmented: int
    procedures_augmented: int
    entries_added: int
