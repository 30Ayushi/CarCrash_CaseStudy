import logging
import pyspark.sql.functions as f

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Analysis5:
    """
    Determine which state has highest number of accidents in which females are not involved.

    Attributes:
        spark (SparkSession): Spark session object.
        data_dict (dict): dictionary that contains dataframes.
        output_path (str): path where the output CSV file will be saved.

    Methods:
        run(): Executes the analysis and saves the results in a CSV file.
    """
    def __init__(self, spark, data_dict, output_path):
        """
        Initializes the Analysis5 class

        Parameters:
            spark (SparkSession): Spark session object.
            data_dict (dict): dictionary that contains dataframes.
            output_path (str): path where the output CSV file will be saved.
        """
        self.spark = spark
        self.data_dict = data_dict
        self.output_path = output_path
    
    def run(self):
        """
        Execute and Save the Analysis for those state which has highest number of accidents in which females are not involved.
 
        Returns:
            pyspark.sql.dataframe.DataFrame: state which has highest number of accidents in which females are not involved
        """
        logger.info ("Fetch the primary_person dataset")
        primary_person_df = self.data_dict['primary_person']

        logger.info("Fetch the units dataset")
        units_df = self.data_dict['units']

        logger.info("Filter the primary_person dataset where female were not involvement")
        females_not_involved_df = primary_person_df.filter(f.col('PRSN_GNDR_ID') != 'FEMALE')

        logger.info("Join the filtered primary dataset with units dataset")
        joined_df = units_df.join(females_not_involved_df,(females_not_involved_df.CRASH_ID == units_df.CRASH_ID) & 
                                                                            (females_not_involved_df.UNIT_NBR == units_df.UNIT_NBR), 'inner')

        logger.info("Group the joined dataset based upon state")
        accidents_state_df = joined_df.groupBy('DRVR_LIC_STATE_ID').count()

        logger.info("Ordering the dataset based upon the highest count of accidents")
        final_dataset = accidents_state_df.orderBy(f.col('count').desc()).select('DRVR_LIC_STATE_ID').limit(1)
   
        logger.info("Write the Output of analayis5 under %s ", self.output_path + "analysis5")
        final_dataset.coalesce(1).write.mode('overwrite').option("header", "true").csv(self.output_path + "analysis5")

        return final_dataset
