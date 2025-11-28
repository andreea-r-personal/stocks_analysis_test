# ReadMe

## Requirements
Before running the ETL pipeline please ensure that your machine has installed:
~~~
mac: python3, java 17/21, apache-spark
win: python3, java 17/21, apache-spark, hardoop
~~~
Create a free account with Polygon: https://polygon.io/docs/stocks/getting-started

## Set-up Environment
This can be run using your local terminal or preferred IDE. 

It is recommended that a virtual environment for python3 is set up for running this code.

1. Navigate to the repository within terminal or IDE.
2. Run command to create environment
~~~
mac: python3 -m venv env
win: python -m venv env
~~~
4. Activate environment:
~~~
mac: source env/bin/activate
win: call env/Scripts/activate
~~~
5. Install required libraries:
~~~
pip install -r requirements.text
~~~

## Running the pipeline
### Set-up workspace
1. Create a local '.env' file containing the following definitions
~~~
MASSIVE_API_KEY = "INSERT_YOUR_MASSIVE_API_KEY_HERE"
START_DATE = "2024-01-01"
END_DATE = "2024-12-31"
LOCAL_RAW = "data/raw/"
LOCAL_PROCESSED = "data/processed/"
LOCAL_OUTPUTS = "outputs/"
RELOAD = "True"
~~~
3. Check that the 'stocks.csv' file is in 'data/raw'. If not, create a local folder named 'data/raw' and add the source file.
4. In terminal or IDE run:
~~~
python main.py
~~~
5. This run can take up to 30 minutes due to rate limitations.
6. Outputs can be found within the outputs folder. Initial run outputs have been provided for convenience.

Note: Files with potentially sensitive information such as raw files, the environment cofiguration or logs have not been commited to this repositoty.


