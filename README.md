# Earthquake Data Pipeline

This repository contains a data engineering project that fetches earthquake data from the USGS Earthquake API, processes it through a Bronze-Silver-Gold medallion architecture using Microsoft Fabric notebooks, and stores the results in Delta tables. The project also includes a historical data notebook and a date dimension table created with Fabric Dataflow Gen 2.

## Project Overview

The project is designed to collect, transform, and enrich earthquake data for analysis. It follows a medallion architecture:

- **Bronze Layer**: Ingests raw earthquake data from the USGS API and stores it in a Delta table.
- **Silver Layer**: Cleans and transforms the raw data, handling null values and converting timestamps.
- **Gold Layer**: Enriches the data with location details (city, state, country) using reverse geocoding and stores the final dataset.
- **Historical Data**: A separate notebook fetches and processes historical earthquake data for a specified date range.
- **Date Dimension**: A Fabric Dataflow Gen 2 generates a date table for time-based analysis.

The pipeline is orchestrated using Microsoft Fabric, executing the Bronze, Silver, and Gold notebooks sequentially, with exit values passed between them.

## Pipeline Architecture

The pipeline consists of three main activities:

1. **Bronze Activity**: Executes `Bronze Nb.ipynb` to fetch raw data and store it in `Bronze_LH.Bronze_data`.
2. **Silver Activity**: Executes `Silver Nb.ipynb` to transform the data and store it in `Silver_LH.Silver_data`.
3. **Gold Activity**: Executes `Gold Nb.ipynb` to enrich the data and store it in `Gold_LH.Gold_data`.

The pipeline screenshot is provided below:

<img width="1414" alt="image" src="https://github.com/abychen01/Earthquake_Analysis_Fabric/blob/ba058b8c103c6dfed1b1a3440795edcb7684b5e1/ETL.png" />

## Repository Structure

- `Bronze Nb.ipynb`: Fetches raw earthquake data from the USGS API.
- `Silver Nb.ipynb`: Cleans and transforms the Bronze data.
- `Gold Nb.ipynb`: Enriches the Silver data with location details.
- `historic_data nb.ipynb`: Fetches and processes historical earthquake data.
- `date_dimension_dataflow.txt`: Power Query code for the date dimension table.
- `pipeline_screenshot.png`: Screenshot of the pipeline in Microsoft Fabric.

## Prerequisites

- **Microsoft Fabric**: Notebooks and pipeline are built in a Fabric environment.
- **Python Libraries**:
  - `requests`: For API calls.
  - `json`: For JSON parsing.
  - `pyspark`: For data processing.
  - `reverse_geocoder`: For reverse geocoding.
  - `pycountry`: For country code conversion.
- **Delta Lake**: For storing data in Bronze, Silver, and Gold layers.
- **Fabric Dataflow Gen 2**: For creating the date dimension table.

## Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/abychen01/Earthquake_Analysis_Fabric.git
cd Earthquake_Analysis_Fabric
```

### 2. Set Up Microsoft Fabric Environment
- Create a Fabric workspace.
- Set up three lakehouses: `Bronze_LH`, `Silver_LH`, and `Gold_LH`.
- Upload the notebooks to your workspace.
- Create an environment in the Fabric workspace for running the notebooks, with pycountry and reverse_geocoder pre-installed.
Custom environment image -
<img width="814" alt="image" src="https://github.com/abychen01/Earthquake_Analysis_Fabric/blob/1aec19d87f54b40cc37e821f344d5a5d17d67fec/custom-env.png" />


### 3. Configure the Pipeline
- Create a pipeline in Fabric matching the structure in `pipeline_screenshot.png`.
- Configure the pipeline to pass exit values (e.g., `bronze_lh`, `silver_lh`, `gold_lh`) between activities.

### 4. Create the Date Dimension Table
- Use the Power Query code in `date_dimension_dataflow.txt` to create the date dimension table in Fabric Dataflow Gen 2.
- Publish the table to your lakehouse.

### 5. Run the Pipeline
- Trigger the pipeline in Fabric to process recent earthquake data.
- Run `historic_data nb.ipynb` separately for historical data.

## Notebooks

- **Bronze Nb.ipynb**: Fetches raw earthquake data for the previous day and stores it in the Bronze layer. Outputs table paths as an exit value.
- **Silver Nb.ipynb**: Reads data from the Bronze layer, cleans it (e.g., handles nulls, converts timestamps), and stores it in the Silver layer.
- **Gold Nb.ipynb**: Reads data from the Silver layer, enriches it with location details using reverse geocoding, and stores it in the Gold layer. 
- **historic_data nb.ipynb**: Fetches historical earthquake data for January to July 2025, processes it through the same transformations, and appends it to the Gold layer.

## Date Dimension Table

The date dimension table is created using Fabric Dataflow Gen 2, spanning January 1, 2022, to December 31, 2025 (1460 days). It includes:
- `Date`
- `Year`
- `Quarter`
- `Month`
- `Week of year`
- `Day`
- `Day name`

See `date_dimension_dataflow.txt` for the Power Query code.

## Troubleshooting

- **Column Name Error in Gold Layer**: The original `Country Code` column name caused an `AnalysisException` due to spaces. Renamed to `CountryCode` in `Gold Nb.ipynb` and `historic_data nb.ipynb`.
- **API Failures**: Ensure the USGS API is accessible and handle rate limits if necessary.
- **Library Issues**: Verify that `reverse_geocoder` and `pycountry` are installed.

## Data Source

This project uses earthquake data retrieved from the [USGS Earthquake API](https://earthquake.usgs.gov/fdsnws/event/1/). The data is provided by the United States Geological Survey (USGS) and is used under their terms, which recommend attribution. Please refer to the [USGS Data Policy](https://www.usgs.gov/data-management/data-policies) for more details.

## License

This project does not use a specific open-source license and is intended for educational and non-commercial purposes only.
