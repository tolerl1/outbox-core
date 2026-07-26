from __future__ import annotations

import os
import re
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

_ASYNCPG_SCHEME = re.compile(r"^postgresql(\+\w+)?://")

_MIGRATION_SQL_PATH = (
    Path(__file__).parent.parent / "src" / "outbox" / "migrations" / "sql" / "0001_initial.sql"
)

_MISSING_DATABASE_URL = """\
OUTBOX_TEST_DATABASE_URL is not set. The integration suite needs a real \
Postgres to run against — point one at it, e.g.:

  docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=outbox postgres:16
  export OUTBOX_TEST_DATABASE_URL=postgresql://postgres:outbox@localhost:5432/postgres
"""


def _as_asyncpg_url(url: str) -> str:
    """Convert a connection URL to use the asyncpg driver.

    Args:
        url (str): database connection URL

    Returns:
        str: URL with postgresql+asyncpg:// scheme
    """
    return _ASYNCPG_SCHEME.sub("postgresql+asyncpg://", url, count=1)


@pytest.fixture(scope="session")
def postgres_url() -> str:
    """Connection URL for the integration suite. Always a real, already-running
    Postgres you point at explicitly — no container gets spun up on your behalf."""
    url = os.environ.get("OUTBOX_TEST_DATABASE_URL")
    if not url:
        pytest.skip(_MISSING_DATABASE_URL)
    return _as_asyncpg_url(url)


@pytest_asyncio.fixture(scope="session")
async def engine(postgres_url: str) -> AsyncIterator[AsyncEngine]:
    """Builds the test schema from the *shipped* migration SQL, not from
    `metadata.create_all` — so the integration suite exercises the actual
    artifact a consumer would `psql -f` against, and any drift between
    `schemas.py` and `0001_initial.sql` shows up as a test failure."""
    eng = create_async_engine(postgres_url)
    sql_script = _MIGRATION_SQL_PATH.read_text(encoding="utf-8")
    async with eng.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS outbox_message CASCADE"))
        # asyncpg can't run a multi-statement string through SQLAlchemy's
        # text() (it prepares statements one at a time), so drop to the raw
        # asyncpg connection, which runs the whole script as one simple query.
        raw_connection = await conn.get_raw_connection()
        driver_connection = raw_connection.driver_connection
        assert driver_connection is not None
        await driver_connection.execute(sql_script)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def clean_outbox_table(engine: AsyncEngine) -> AsyncIterator[None]:
    """Truncate outbox_message before each test.

    Args:
        engine (AsyncEngine): the test database engine

    Yields:
        None: control returns to test after truncation
    """
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE outbox_message RESTART IDENTITY"))
    yield


@pytest.fixture
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Provide an async SQLAlchemy session factory bound to the test database.

    Returns:
        async_sessionmaker[AsyncSession]: A factory for creating async database sessions.
    """
    return async_sessionmaker(engine, expire_on_commit=False)
