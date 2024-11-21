from sqlalchemy import create_engine, text
import logging
import pandas as pd
from data_ingestion import create_db_engine, query_data, read_from_web_CSV


# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger('data_ingestion')
# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

### START FUNCTION

class FieldDataProcessor:

    def __init__(self, config_params, logging_level="INFO"):  # Make sure to add this line, passing in config_params to the class 
        self.db_path = config_params['db_path']
        self.sql_query = config_params["sql_query"]
        self.columns_to_rename = config_params["columns_to_rename"]
        self.values_to_rename = config_params["values_to_rename"]
        self.weather_map_data = config_params["weather_mapping_csv"]

        self.initialize_logging(logging_level)

        # We create empty objects to store the DataFrame and engine in
        self.df = None
        self.engine = None

    # This method enables logging in the class.
    def initialize_logging(self, logging_level):
        """
        Sets up logging for this instance of FieldDataProcessor.
        """
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False  # Prevents log messages from being propagated to the root logger

        # Set logging level
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":  # Option to disable logging
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO  # Default to INFO

        self.logger.setLevel(log_level)

        # Only add handler if not already added to avoid duplicate messages
        if not self.logger.handlers:
            ch = logging.StreamHandler()  # Create console handler
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

        # Use self.logger.info(), self.logger.debug(), etc.


    # let's focus only on this part from now on
    def ingest_sql_data(self):
        """
        Loads data from the SQL database into the DataFrame (self.df).
        """
        try:
            # Initialize the database engine
            self.engine = create_db_engine(self.db_path)
            # Load the data using the query and store it in the DataFrame
            self.df = query_data(self.engine, self.sql_query)
            # Log the successful data load
            self.logger.info("Successfully loaded data into the DataFrame.")
            return self.df  # Return the populated DataFrame
        except Exception as e:
            # Log the error and re-raise it
            self.logger.error(f"Failed to ingest SQL data. Error: {e}")
            raise e


    def rename_columns(self):
         # Extract the columns to rename from the configuration
        column1, column2 = list(self.columns_to_rename.keys())[0], list(self.columns_to_rename.values())[0]
        
        self.logger.info(f"Swapped columns: {column1} with {column2}")

        # Temporarily rename one of the columns to avoid a naming conflict
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"

        # Perform the swap
        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})    

    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        self.df[abs_column] = self.df[abs_column].abs()
        self.df[column_name] = self.df[column_name].apply(lambda crop: self.values_to_rename.get(crop, crop))

    def weather_station_mapping(self):
        try:
            # Read the weather station mapping data from the web
            weather_mapping_df = read_from_web_CSV(self.weather_map_data)
            
            # Merge the weather mapping data into the main DataFrame on 'Field_ID'
            self.df = self.df.merge(
                weather_mapping_df, 
                on="Field_ID", 
                how="left"  # Use a left join to retain all fields in the main DataFrame
            )
        
            # Log the successful merge
            self.logger.info("Successfully merged weather station mapping data into the main DataFrame.")
        except Exception as e:
            # Log the error and re-raise it
            self.logger.error(f"Failed to merge weather station mapping data. Error: {e}")
            raise e
        
    def process(self):
        """
        Executes the full data processing pipeline step-by-step.
        """
        try:
            # Step 1: Ingest SQL data
            self.logger.info("Starting the data processing pipeline.")
            self.ingest_sql_data()
            
            # Step 2: Rename columns
            self.logger.info("Renaming columns...")
            self.rename_columns()
            
            # Step 3: Apply corrections to the data
            self.logger.info("Applying data corrections...")
            self.apply_corrections()
            
            # Step 4: Map weather station data
            self.logger.info("Mapping weather station data...")
            self.weather_station_mapping()
            
            # Final Step: Log success
            self.logger.info("Data processing pipeline completed successfully.")
        except Exception as e:
            self.logger.error(f"Error occurred during the data processing pipeline: {e}")
            raise e
    
### END FUNCTION