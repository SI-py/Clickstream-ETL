import argparse
import logging

import uvicorn

from app.etl.pipeline import run_full_etl, run_incremental_etl

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def main():
    parser = argparse.ArgumentParser(description="Clickstream ETL entrypoint")
    parser.add_argument(
        "command",
        choices=["api", "full", "incremental"],
        help="api: run FastAPI server, full: run full snapshot ETL, incremental: run incremental ETL",
    )
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    if args.command == "api":
        uvicorn.run("app.web.main:app", host=args.host, port=args.port, reload=False)
        return

    if args.command == "full":
        result = run_full_etl()
    else:
        result = run_incremental_etl()

    logging.getLogger(__name__).info("Done: %s", result)


if __name__ == "__main__":
    main()
