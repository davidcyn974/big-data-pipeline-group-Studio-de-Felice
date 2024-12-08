# Documentation: Data Preprocessing and Cleaning

This notebook cleans and preprocess the given data using PySpark. It is designed to handle missing values, remove duplicates, normalize formats, and filter irrelevant data, preparing the dataset for analysis or machine learning.

## Notebook Workflow

1. **Setup**
   - Install required libraries (`pyspark`).
   - Import necessary modules from PySpark.

2. **Data Loading**
   - Establish a Spark session.
   - Load the input dataset (`ecommerce_data_with_trends.csv`) and display its schema, sample rows, and size.

3. **Data Preprocessing**
   - **Handle Missing Values**: Remove rows with null values in critical columns (`transaction_id`, `timestamp`, `customer_id`, `total_amount`).
   - **Remove Duplicates**: Eliminate duplicate rows.
   - **Filter Invalid Rows**: Retain only rows where the `total_amount` is greater than 0.

4. **Data Cleaning**
   - **Timestamp Normalization**: Convert the `timestamp` column to a consistent datetime format (`yyyy-MM-dd HH:mm:ss`).
   - **Customer Type Standardization**: Ensure the `customer_type` column is in lowercase and trimmed of whitespace.

5. **Export**
   - Save the cleaned and preprocessed data to a CSV file (`preprocessed_data.csv`).


## Code Highlights
- Utilizes PySpark's DataFrame API for scalable data processing.
- Demonstrates practical data cleaning techniques, such as handling nulls, normalizing formats, and filtering invalid records.


## Output
- The cleaned dataset is exported to `../preprocessed_data`.