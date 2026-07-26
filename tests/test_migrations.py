"""Tests for schema migrations and correctness."""

from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from outbox.migrations import ddl
from outbox.schemas import outbox_message


def test_ddl_loads_the_shipped_migration_sql() -> None:
    """Verify that ddl() reads the packaged SQL file with the expected objects."""
    sql = ddl()

    assert "CREATE TABLE outbox_message" in sql
    assert "ix_outbox_pending" in sql
    assert "ix_outbox_claimed_lease" in sql
    assert "ix_outbox_partition_key_blocking" in sql
    assert "ck_outbox_message_status" in sql


def test_check_constraint_name_expands_without_doubling() -> None:
    """Verify the naming convention expands 'status' to ck_outbox_message_status."""
    compiled = str(CreateTable(outbox_message).compile(dialect=postgresql.dialect()))

    assert "ck_outbox_message_status" in compiled
    assert "ck_outbox_message_ck_outbox_message_status" not in compiled
