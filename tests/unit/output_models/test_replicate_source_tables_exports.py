from shared.output_models import ReplicateSourceTablesOutput, ReplicateTableResult


def test_replicate_source_tables_models_export_from_barrel() -> None:
    output = ReplicateSourceTablesOutput(
        status="ok",
        dry_run=True,
        limit=10,
        tables=[
            ReplicateTableResult(
                fqn="silver.dimcustomer",
                source_schema="silver",
                source_table="DimCustomer",
                target_schema="bronze",
                target_table="DimCustomer",
                status="planned",
            )
        ],
    )

    assert output.tables[0].fqn == "silver.dimcustomer"
