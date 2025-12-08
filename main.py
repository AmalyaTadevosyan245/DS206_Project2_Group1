"""
main.py

Entry point for executing the dimensional ETL pipeline from the command line.

Usage:
    python main.py --start_date=1995-01-01 --end_date=1997-12-31
"""

import argparse
from pipeline_dimensional_data.flow import DimensionalDataFlow


def parse_args():
    parser = argparse.ArgumentParser(description="Run Dimensional ETL Pipeline")

    parser.add_argument(
        "--start_date",
        required=True,
        type=str,
        help="Start date for fact ingestion (YYYY-MM-DD)"
    )

    parser.add_argument(
        "--end_date",
        required=True,
        type=str,
        help="End date for fact ingestion (YYYY-MM-DD)"
    )

    return parser.parse_args()


def main():
    args = parse_args()

    pipeline = DimensionalDataFlow()

    result = pipeline.exec(
        start_date=args.start_date,
        end_date=args.end_date
    )

    if result["success"]:
        print(f"Pipeline executed successfully. Execution ID: {result['execution_id']}")
    else:
        print(f"Pipeline FAILED. Execution ID: {result['execution_id']}")


if __name__ == "__main__":
    main()