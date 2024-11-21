from sqlalchemy import create_engine, text
import logging
import pandas as pd

# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger('data_ingestion')
# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')



### START FUNCTION

def create_db_engine(db_path):
    """
    Creates and returns a SQLAlchemy database engine for the specified database path.

    This function establishes a connection to a database using SQLAlchemy. It tests
    the connection to ensure the engine was created successfully. If an error occurs,
    the function logs the error and raises the appropriate exception.

    Parameters:
        db_path (str): The database connection string in SQLAlchemy's URI format.

    Returns:
        sqlalchemy.engine.base.Engine: A SQLAlchemy Engine instance for the specified database.

    Raises:
        ImportError: If SQLAlchemy is not installed.
        Exception: For any other errors encountered while creating the database engine.
    """

    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine # Return the engine object if it all works well
    except ImportError: #If we get an ImportError, inform the user SQLAlchemy is not installed
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise e
    except Exception as e:# If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e
    
def query_data(engine, sql_query):
    """
    Executes a SQL query on the specified database engine and returns the results as a Pandas DataFrame.

    This function uses a SQLAlchemy engine to connect to the database, executes the given SQL query, 
    and returns the results as a Pandas DataFrame. If the query returns no data, an error is raised.

    Parameters:
        engine (sqlalchemy.engine.base.Engine): A SQLAlchemy Engine instance connected to the database.
        sql_query (str): The SQL query to be executed.

    Returns:
        pandas.DataFrame: A DataFrame containing the results of the SQL query.

    Raises:
        ValueError: If the query returns an empty DataFrame.
        Exception: For any other errors encountered while executing the query.

    Example:
        >>> engine = create_db_engine("sqlite:///example.db")
        >>> sql_query = "SELECT * FROM users WHERE age > 30"
        >>> df = query_data(engine, sql_query)
        >>> print(df.head())
    """

    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e: 
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e
    
def read_from_web_CSV(URL):
    """
    Reads a CSV file from a web URL into a Pandas DataFrame.

    This function takes a URL pointing to a CSV file, reads the file, and returns its contents 
    as a Pandas DataFrame. If the URL does not point to a valid CSV file, or if there is any 
    other issue while reading the file, appropriate exceptions are raised.

    Parameters:
        URL (str): The web URL pointing to the CSV file.

    Returns:
        pandas.DataFrame: A DataFrame containing the data from the CSV file.

    Raises:
        pandas.errors.EmptyDataError: If the URL does not point to a valid CSV file or the file is empty.
        Exception: For any other errors encountered while reading the file.

    Example:
        >>> url = "https://example.com/data.csv"
        >>> df = read_from_web_CSV(url)
        >>> print(df.head())
    """

    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e
    
### END FUNCTION