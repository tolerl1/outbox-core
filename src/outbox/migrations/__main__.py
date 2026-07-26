from __future__ import annotations

from outbox.migrations import ddl


def main() -> None:
    print(ddl(), end="")


if __name__ == "__main__":
    main()
