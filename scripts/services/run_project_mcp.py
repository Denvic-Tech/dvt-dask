import argparse
import sys

from services.project_mcp.main import run_project_mcp


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run project_mcp server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse", "streamable-http"],
        default="stdio",
    )
    parser.add_argument(
        "--no-reexec",
        action="store_true",
        help="Do not restart with resolved project interpreter",
    )
    return parser.parse_args()


if __name__ == "__main__":
    if sys.version_info < (3, 10):
        raise SystemExit("project_mcp requires Python 3.10 or newer")

    args = _parse_args()
    run_project_mcp(transport=args.transport, ensure_project_interpreter=not args.no_reexec)
