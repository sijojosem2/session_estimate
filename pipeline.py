import boto3
import config


boto3_session = boto3.Session(
    aws_access_key_id=config.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=config.AWS_SECRET_ACCESS_KEYS,
)


API_KEY = config.API_KEY

""" 
    ------------------  Quick Referenece  ------------------

    Common Key Attributes:
    ---------------------------------------
    "dataset_name"  : << used to designate the dataset and name the csv make sure to use no spaces so that when writing csv the dataset can be identified >>
    "description"   : << a simple description of the dataset and/or its source>>
    
    
    Other Key Attributes:
    ---------------------------------------
    "write_to_csv"  : << Flag enables or disables csv creation from the pandas dataframe>> 
    "exec_sql"      : << Designates an SQL file to be executed in the pipeline sequence>> 
    "aws_s3_boto"   : << Created for handling S3 datasets, this uses boto3 for session and aws profile handling>> 
    "request"       : << url and headers to be provided here, any additional parameters, body also should be given >>
    "pd_dataframe"  : << pandas dataframe parameters, for JSON responses, provide the column that needs to be extracted in 'record_path' ;
                        for non JSON responses provide either the column delimiter (variable) or colspecs (fixed length) other parameters like delimiter,
                        column separators or column names are derived from the pandas documentation:
                        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html >>
    "target_table"  : <<target table parameters with reference to the pandas to_sql is given here>>
    

"""

input = {
    "desctiption": "etl_pipeline",
    "etl": [
        {
            "dataset_name": "pre_execution",
            "description": "file execution pre etl",
            "exec_sql": {"name": "pre_exec.sql"}
        },
        {
            "dataset_name": "local_file",
            "description": "local tier session",
            "local": "local_files/mobile_events_2020.csv",
            "write_to_csv": False,
            "var_length": True,
            "pd_dataframe": {
                "params": {
                    "sep": ";", "header": "infer"},
                "drop_cols": []
            },
            "target_table": {"name": "mobile_events", "if_exists": "append", "index": False}
        },
        {
            "dataset_name": "post_execution_sql_script_example",
            "description": "file execution post etl",
            "exec_sql": {"name": "post_exec.sql"}
        },

    ]
}
