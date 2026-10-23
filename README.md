# E-commerce Lakehouse & Analytics Platform

An end-to-end data engineering project that processes e-commerce data through a **Bronze, Silver, and Gold** pipeline to generate analytics-ready datasets.

## Architecture

`Source CSVs → Bronze → Silver → Gold → Power BI`

- **Bronze:** Raw input data
- **Silver:** Cleaned and validated data
- **Gold:** Business analytics datasets

## Tech Stack

- Python
- PySpark 3.5.3 and Java 17
- CSV
- Power BI
- Python `unittest`

## Project Structure

```text
ecommerce_lakehouse_minimal/
├── data/sample/       # Sample input CSV files
├── src/               # Pipeline code
├── tests/             # Automated tests
├── powerbi/            # Dashboard guide
├── output/             # Generated outputs
├── requirements.txt
└── README.md
```

## Run Locally

Run these commands from the project root:

```powershell
python src/run_pipeline.py --input data/sample --output output
python -m unittest discover -s tests -v
```

Generated analytics files are available in `output/gold/` and can be imported into Power BI.

## Future Improvements

- PySpark-based transformations and Parquet storage
- Incremental processing and Delta Lake
- CI/CD and pipeline monitoring

## Author

**Alka Mishra**  
[GitHub](https://github.com/Alka2-mishra) · [LinkedIn](https://www.linkedin.com/in/alka-mishra-a34408310/)
