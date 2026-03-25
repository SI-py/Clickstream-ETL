import logging

from app.etl.pipeline import run_full_etl

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def main():
    result = run_full_etl()
    logging.getLogger(__name__).info("Done: %s", result)


if __name__ == "__main__":
    main()
