import io
import ssl
import requests
import pandas as pd
import awswrangler as wr
from datetime import datetime

import config
from pipeline import input
from models.dbops import DbOps
from models.logerreventhandler import LogErrEventHandler

ssl._create_default_https_context = ssl._create_unverified_context

LOG_FILE_NAME = 'etl_logs'

ABS_CSV = (config.CSV_DIR)
ABS_LOG = (f"{config.LOG_DIR}/{LOG_FILE_NAME}_{datetime.now().strftime('%Y%m%d')}.txt")

database = DbOps()
logging  = LogErrEventHandler(ABS_LOG)
logger   = logging.logwrite()


@logging.exceptionhandle
def get_data_url(*args, **kwargs):
    """
    Simple response function to GET the data set
    """

    return requests.get(**kwargs)


@logging.exceptionhandle
def exec_sql(*args, **kwargs):
    """
    Executes the sql file provided in the input stream
    """
    return database.execute_sql_file(**kwargs)


@logging.exceptionhandle
def db_insert(df, *args, **kwargs):
    """
    Inserts Records from the created Dataframe
    """
    return database.insert_records(df, **kwargs)


@logging.exceptionhandle
def create_csv(df, *args, csv_dir=ABS_CSV, **kwargs):
    """
    Optional, creates a csv in the given input folder if specified
    """

    df.to_csv(csv_dir + kwargs['dataset_name'] + '.csv')

    logger.debug("csv creation done for {} with {} records, written to {}".format(kwargs["dataset_name"], len(df), csv_dir + kwargs['dataset_name'] + '.csv'))

    return


@logging.exceptionhandle
def aws_s3_boto(*args, **kwargs):
    """
    Inserts Records from S3
    """
    df = wr.s3.read_parquet(kwargs["aws_s3_boto"]["path"], boto3_session=kwargs["aws_s3_boto"]["boto3_session"])

    logger.debug("Dataframe for {} with {} records created from JSON".format(kwargs["dataset_name"], len(df)))

    if kwargs['write_to_csv']:
        create_csv(df, **kwargs)

    return df


@logging.exceptionhandle
def extract_data_local(*args, **kwargs):


    dataset = kwargs['local']

    if 'var_length' in kwargs.keys():

        df = pd.read_csv(dataset, **kwargs['pd_dataframe']['params']).drop(kwargs['pd_dataframe']['drop_cols'], axis=1)
        logger.debug("Dataframe for {} with {} records created from variable length csv".format(kwargs["dataset_name"], len(df)))

    elif 'fixed_length' in kwargs.keys():
        df = pd.read_csv(dataset,  **kwargs['pd_dataframe']['params']).drop(kwargs['pd_dataframe']['drop_cols'], axis=1)
        logger.debug("Dataframe for {} with {} records created from fixed length csv".format(kwargs["dataset_name"], len(df)))

    else:
        logger.debug("Condition Not covered, please check input")

    return df



@logging.exceptionhandle
def extract_data(*args, **kwargs):
    """
    Returns Pandas Dataframe based on the input file conditions, can handle JSON, variable
    length and fixed length return types Normalisation and drop columns must be supplied in
    the input until I figure out a great method to handle JSONB or Arrays
    """

    dataset = get_data_url(**kwargs['request'])
    df = pd.DataFrame()

    if (dataset.text)[0] in '{[':
        if kwargs['pd_dataframe']['norm']:
            df = pd.json_normalize(data=dataset.json(), **kwargs['pd_dataframe']['norm']).drop( kwargs['pd_dataframe']['drop_cols'], axis=1)
            logger.debug("Dataframe for {} with {} records created from JSON".format(kwargs["dataset_name"], len(df)))

        else:
            df = pd.json_normalize(data=dataset.json())
            logger.debug("Dataframe for {} with {} records created from JSON".format(kwargs["dataset_name"], len(df)))


    elif 'var_length' in kwargs.keys():
        df = pd.read_csv(io.StringIO(dataset.text), **kwargs['pd_dataframe']['params']).drop( kwargs['pd_dataframe']['drop_cols'],axis=1)
        logger.debug( "Dataframe for {} with {} records created from variable length csv".format(kwargs["dataset_name"], len(df)))


    elif 'fixed_length' in kwargs.keys():
        df = pd.read_fwf(io.StringIO(dataset.text), **kwargs['pd_dataframe']['params']).drop( kwargs['pd_dataframe']['drop_cols'], axis=1)
        logger.debug( "Dataframe for {} with {} records created from fixed length csv".format(kwargs["dataset_name"], len(df)))

    else:
        logger.debug("Condition Not covered, please check input")


    if kwargs['write_to_csv']:
        create_csv(df, **kwargs)

    return df


@logging.exceptionhandle
def main():

    logger.debug("------------- STARTING ETL PROCESS ----------------------------------")

    for i in input['etl']:
        if "exec_sql" in i.keys():
            exec_sql(**i)
            logger.debug("{} Execution done, moving on ....".format(i["dataset_name"]))

        elif "aws_s3_boto" in i.keys():
            db_insert(aws_s3_boto(logger, **i), **i)
            logger.debug("{} Execution done, moving on ....".format(i["dataset_name"]))

        elif "request" in i.keys():
            db_insert(extract_data(logger, **i), **i)
            logger.debug("{} Execution done, moving on ....".format(i["dataset_name"]))

        elif "local" in i.keys():
            db_insert(extract_data_local(logger, **i), **i)
            logger.debug("{} Execution done, moving on ....".format(i["dataset_name"]))

        else:
            logger.debug("no matching condition found")


    logger.debug("--------------- ENDING ETL PROCESS ----------------------------------")


if __name__ == "__main__":
    main()
