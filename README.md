# Data Subscriber App

The **Data Subscriber App** is a streaming data processing solution built with PySpark, Delta Lake, and AWS Kinesis. It processes real-time data from Kinesis streams and performs various transformations and aggregations before saving the results to Delta tables on AWS S3. This app is designed to efficiently handle streaming data for large-scale data pipelines.

## Table of Contents

1. [Overview](#overview)
2. [Project Structure](#project-structure)
3. [Setup](#setup)
4. [Usage](#usage)
5. [Components](#components)
6. [Logging](#logging)
7. [Contributing](#contributing)
8. [License](#license)

## Overview

This application subscribes to a Kinesis stream, processes incoming orders, and updates related data in a real-time pipeline. The processed data is then stored in Delta tables for further analysis and reporting.

### Features:

- Real-time data ingestion from AWS Kinesis.
- Streaming data processing and transformation using PySpark.
- Data persistence in Delta tables on AWS S3.
- Flexible architecture with easily extendable components.
- Managed dependencies with Poetry for easier package management.

## Project Structure

```
project-root/
│
├── main.py                 # Main entry point for the application.
├── spark_session.py         # Module for creating and managing the Spark session.
├── functions/               # Directory containing all the data processing logic.
├── schemas.py               # PySpark schema definitions for the Delta tables.
├── requirements.txt         # Alternative to Poetry dependencies, if needed.
└── pyproject.toml           # Poetry configuration file.
```

## Setup

### Prerequisites:

- **Python 3.10.x**: Ensure you have Python installed.
- **Poetry**: Used for managing dependencies and virtual environments.
- **Java 11**: Required for running PySpark.

### Installation Steps:

1. **Clone the repository**:

   ```bash
   git clone https://github.com/mgranica/VPR-data_publisher
   cd VPR-data_publisher
   ```

2. **Install Poetry**:

   If you don’t have Poetry installed, run:

   ```bash
   pip install poetry
   ```

3. **Install dependencies**:

   Use Poetry to install all required dependencies:

   ```bash
   poetry install
   ```

4. **Activate the virtual environment**:

   After installing the dependencies, activate the virtual environment:

   ```bash
   poetry shell
   ```

5. **Set up AWS credentials**:

   The app requires AWS credentials to access the Kinesis stream and S3. You can set these up in two ways:
   
   - **Local Development**: Store your AWS credentials in the `~/.aws/credentials` file.
   - **GitHub Actions**: Use the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

6. **Configure AWS settings**:

   Modify the `.aws/config` file to include your `REGION`, `STREAM_NAME`, and any other necessary configuration values.

## Usage

After setting up the environment, you can run the app with:

```bash
python src/main.py
```

The app will:

- Create a Spark session using the AWS credentials and configurations.
- Fetch data from the Kinesis stream.
- Process and save the data to Delta tables.
- Continuously monitor the Kinesis stream and update the Delta tables in real-time.

## Components

### 1. **`main.py`**

This is the main entry point of the app. It initializes the Spark session, loads AWS credentials and configuration, and orchestrates the processing of data streams. The `main` function ties together the components to process different streams.

### 2. **`spark_session.py`**

This module handles the creation of a Spark session. It sets up the necessary configurations for AWS S3, Delta Lake, and any required Spark packages (e.g., Hadoop for S3 integration).

- Function: `create_spark_session(aws_access_key_id, aws_secret_access_key)`

### 3. **Data Processors**

Located in the `functions/` directory, each processor handles specific parts of the data pipeline:

- **KinesisDataProcessor**: Fetches and saves data from AWS Kinesis.
- **OrdersItemsProcessor**: Processes orders and items data streams.
- **EventsStreamProcessor**: Handles the events data stream.
- **OrdersStreamProcessor**: Processes orders-related data.
- **PackagesStreamProcessor**: Handles package updates in the stream.

### 4. **`schemas.py`**

Contains the PySpark schema definitions for the Delta tables used in the app. Each schema corresponds to a different table (e.g., `orders`, `orders_items`, `packages`).

- `orders_schema`: The schema used for the Kinesis data fetch.

### 5. **Logging**

Logging is set up using the `setup_logging()` function in `main.py`. It logs both to the console and to a file (`app.log`) in the project root.

You can adjust the log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) depending on your needs.

### 6. **AWS Credentials Management**

- **Local Development**: AWS credentials are loaded from the `.aws/credentials` file.
- **GitHub Actions**: Credentials are sourced from environment variables.

### 7. **Checkpoints and Data Paths**

The app relies on several checkpoints to track data stream progress and avoid reprocessing. These checkpoints and Delta table paths are configurable and stored in AWS S3.

- **Checkpoints**: Keep track of streaming progress for each data source.
- **Paths**: Define where Delta tables are stored (e.g., orders, events, packages).

## Logging

Logs are stored in both the console and a file (`app.log`). You can adjust the logging level as per your requirements. The logging configuration is managed in `setup_logging()` in the main script.

## Contributing

Contributions are welcome! Please submit pull requests or issues through the repository's issue tracker.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
