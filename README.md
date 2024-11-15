# Crash Analysis Project

This project performs detailed analysis on accident/crash data using PySpark. The analysis focuses on various factors such as vehicle types, crash severity, driver details, and geographic factors. The project includes 10 different analyses aimed at uncovering trends and insights from the provided datasets.

## Prerequisites

Before running the project, ensure that you have the following software installed:

- **Apache Spark** (version 3.x) with PySpark
- **Hadoop** (for file system support, e.g., HDFS or local)
- **Java** (for Spark to run)
- **Python 3.x** (preferably 3.8+)
- **PySpark**
- **YAML** (for configuration management)

### Installing Dependencies

To set up the environment, install the required dependencies using:

```bash
pip install pyspark
pip install pyyaml
```

## Project Setup

1. **Clone the repository** (or upload your project files).
2. **Configure the data paths**: Edit the `config.yaml` file with the correct paths to your CSV files. The data should be accessible from the location where the script is being executed.

Example `config.yaml`:

```yaml
data_paths:
  primary_person: /path/to/primary_person.csv
  units: /path/to/units.csv
  charges: /path/to/charges.csv
  endorse: /path/to/endorsements.csv
  restrict: /path/to/restrictions.csv
  damages: /path/to/damages.csv
output_path: /path/to/output/directory
```

Ensure that all CSV files are formatted correctly with headers, and the appropriate schema is inferred during reading.

### Project Structure
project/ 
├── data/ # Folder for input data files 
├── main/ # Source code for data processing and analysis 
├── config/ # Configuration files 
└── output/ # Output results from analysis

## Running the Analysis

1. **Submit the Spark job**:

   Use the following command to run the script with Spark:

   ```bash
   spark-submit --conf "spark.driver.extraJavaOptions=-Djava.security.manager=allow" --conf "spark.executor.extraJavaOptions=-Djava.security.manager=allow" main/analytics.py
   ```

   This command will execute the analytics, where `main/analytics.py` is the path to your Python script.

2. **Output**:

   After running the script, the analysis results will be printed to the console for each of the 10 analyses.

   The output will include:
   - Counts of crashes with certain conditions (e.g., number of deaths, type of vehicles involved).
   - Top vehicle makes or models involved in crashes.
   - The number of crashes in specific zip codes or states.
   - The vehicle makes involved in speeding-related offenses.

## Analysis Overview

The project performs the following analyses:

1. **Analysis 1**: Counts crashes where more than 2 males were killed.
2. **Analysis 2**: Counts how many motorcycles were involved in crashes.
3. **Analysis 3**: Lists the top 5 vehicle makes of cars where the driver died and airbags did not deploy.
4. **Analysis 4**: Determines the number of vehicles with valid licenses involved in hit-and-run cases.
5. **Analysis 5**: Finds which state has the highest number of crashes where females are not involved.
6. **Analysis 6**: Lists the top 3rd to 5th vehicle makes contributing to the largest number of injuries.
7. **Analysis 7**: Identifies the top ethnic user group for each vehicle body style.
8. **Analysis 8**: Lists the top 5 zip codes with the most alcohol-related crashes.
9. **Analysis 9**: Counts the number of distinct crash IDs with no damaged property but a damage level above 4 and with insurance.
10. **Analysis 10**: Determines the top 5 vehicle makes involved in speeding-related offenses with licensed drivers and cars in the top 25 states.

## Code Overview

### SparkSession Setup

The `SparkSession` is initialized with configurations such as local execution (`local[*]`), memory settings, and compression options. Logging is set to "WARN" to reduce verbosity.

### `CrashAnalysis` Class

This class handles the entire crash analysis workflow. It includes methods for each of the analyses mentioned above. The main functionalities of this class include:
- Reading the input data from CSV files.
- Performing aggregation, joins, filtering, and other transformations using PySpark.
- Outputting the results for each analysis.

### Helper Methods

- `load_config()`: Loads the configuration file containing the data paths and output directory.
- `spark-submit` command: A Spark job submission command to run the analysis on the cluster or locally.

## Conclusion

This project provides deep insights into accident and crash data through various types of analyses. It can be expanded to include more features, data sources, and visualizations as required.

## Contribution

Feel free to fork this repository, make changes, and submit pull requests for improvements or additional analyses.

Let me know if you need additional details or modifications in the README file!
