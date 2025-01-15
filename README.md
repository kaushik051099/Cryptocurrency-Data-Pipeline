# Cryptocurrency Data Pipeline

This project implements an automated ETL (Extract, Transform, Load) pipeline using Apache Airflow to collect, process, and analyze cryptocurrency price data. The pipeline fetches data from the CoinGecko API, processes it, and stores it in a PostgreSQL database for further analysis.

## Features

- Automated data collection from CoinGecko API
- Real-time price, volume, and price change tracking for major cryptocurrencies
- Data transformation and cleaning
- Automated metric calculation and insights generation
- Data persistence in PostgreSQL database
- Hourly schedule with error handling and retries

## Technical Stack

- Apache Airflow
- Python 3.8+
- PostgreSQL
- pandas
- requests

## Project Structure

```
├── dags/
│   └── crypto_pipeline.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Setup Instructions

1. Clone the repository:
```bash
git clone https://github.com/yourusername/crypto-etl-pipeline.git
cd crypto-etl-pipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up Airflow connections:
   - Add PostgreSQL connection with ID 'postgres_default'
   - Add HTTP connection with ID 'coingecko_api' pointing to 'https://api.coingecko.com'

4. Start Airflow:
```bash
docker-compose up -d
```

5. Access Airflow UI at `http://localhost:8080`

## Pipeline Workflow

1. **API Health Check**: Verifies CoinGecko API availability
2. **Database Setup**: Creates required tables if they don't exist
3. **Data Extraction**: Fetches current cryptocurrency prices and metrics
4. **Data Processing**: Transforms raw data into structured format
5. **Metrics Calculation**: Generates insights and additional metrics
6. **Data Loading**: Stores processed data in PostgreSQL

## Contributing

Feel free to submit issues, fork the repository, and create pull requests for any improvements.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
