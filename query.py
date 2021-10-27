from datetime import datetime

import config
from models.dbops import DbOps
from models.logerreventhandler import LogErrEventHandler


LOG_FILE_NAME = 'query_logs'

ABS_LOG = (f"{config.LOG_DIR}/{LOG_FILE_NAME}_{datetime.now().strftime('%Y%m%d')}.txt")
logger  = LogErrEventHandler(ABS_LOG).logwrite()


USER_INPUT = """ 

1 -  Run SQL Query
2 -  Exit
 
Enter choice :
"""




database = DbOps()


def run_query():
    try:
        sql_query = input("\nEnter Query: \n")
        print(database.execute_console_qry(sql_query))
    except Exception as e:
        print(f"{e} query error, check and try again")
        logger.debug(f" Error {e} for query {sql_query}")
    return


USER_OPTIONS = {

    "1": run_query


}


def main():


    while (selection := input(USER_INPUT)) != "2":
        try:
            USER_OPTIONS[selection]()
        except KeyError:
            print("Invalid Choice, try again or hit 2 for exit")
            logger.debug(f"user keyed input : {selection}")



if __name__ == "__main__":
    main()