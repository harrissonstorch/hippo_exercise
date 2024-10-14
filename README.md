# Hippo Exercise

This project analyzes pharmacy claims and generates metrics and recommendations.

## Requirements

- Python 3.9 or higher

## Execution

I created two different scripts to showcase different approaches for the proposed exercise.

- Python: This uses simple python code with basic python libraries, which will work better for small files, for an exercise.

- Dask: This uses Dask library to parallelize the work, but also to showcase the use of pandas and sql options to get the same result. This would likely the direction I would choose for a production pipeline.

### Steps

1. Clone the repository:

   ```bash
   git clone https://github.com/harrissonstorch/hippo_exercise.git
   cd hippo_exercise
   ```

2. Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

3. Run main():

   ```bash
   python3 src/main.py
    ````
    Or
    ```bash
   python3 src/main_dask.py
    ```


    By default, the script will use the sample files provided in /data directory. Optionally, you can set 3 input parameters (pharmacy,claims,reverts) as lists. For example:

    ```bash
    python3 src/main.py --pharmacy_dirs "path/to/files1" "path/to/files2" --claims_dirs "path/to/claims" --reverts_dirs "path/to/reverts"
    ```

4. Check the results:

    ```bash
    cd results
    ```

    The results will be available in the following files:

    ```
    metrics.json
    recommendations.json
    quantity_prescriptions.json
    ```
    Or
    ```
    metrics_dask.json
    recommendations_dask.json
    quantity_prescriptions_dask.json
    ```


## Notes

- It was mentioned that claims and reverts are streaming data. This app it's a batch program, so another program would need to control the execution and outputs managing the input parameters.
- For a production scenario, I would initially think of a solution using spark streaming or Delta Live tables, where analysts could use SQL to get the data. Similarly, it could be done with Kinesis/Flink.
- Moving to the analytical part of the project, I noticed we have in the sample data events that are 5 months apart and we are not using the time dimension for the events, and since we are talking about streaming events, I would try to understand the specific use cases for those metrics, to consider suggesting we focus on more recent data, based on timestamp and/or last n events. 


