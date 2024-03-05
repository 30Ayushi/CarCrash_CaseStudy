import logging
import pyspark.sql.functions as f

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Analysis3:
    """
    Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.

    Attributes:
        spark (SparkSession): Spark session object.
        data_dict (dict): dictionary that contains dataframes.
        output_path (str): path where the output CSV file will be saved.

    Methods:
        run(): Executes the analysis and saves the results in a CSV file.
    """
    def __init__(self, spark, data_dict, output_path):
        """
        Initializes the Analysis3 class

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
        Execute and Save the Analysis of Top 5 Vehicle Makes of the cars present in the crashes where driver died and Airbags did not deploy
 
        Returns:
            pyspark.sql.dataframe.DataFrame: Top 5 VEH_MAKE_ID who where involed in crashes and where driver died and Airbags did not deploy
        """
        logger.info ("Fetch the primary_person dataset")
        primary_person_df = self.data_dict['primary_person']

        logger.info("Fetch the units dataset")
        units_df = self.data_dict['units']

        logger.info("Filter the data from primary_person where driver is dead and Airbags are not deployed.")
        filtered_df = primary_person_df.filter((f.col('PRSN_TYPE_ID') == 'DRIVER') &
                                                (f.col('PRSN_INJRY_SEV_ID') == 'KILLED') &
                                                (f.col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED'))

        logger.info("Join the filtered data with units data in order to fetch VEH_MAKE_ID")
        combined_df = filtered_df.join(units_df, (filtered_df.CRASH_ID == units_df.CRASH_ID) & 
                                                    (filtered_df.UNIT_NBR == units_df.UNIT_NBR), 'inner')

        logger.info("Group the combined data based on VEH_MAKE_ID")
        top_5_makes_df = combined_df.groupBy("VEH_MAKE_ID").count().orderBy(f.col("count").desc()).select('VEH_MAKE_ID').limit(5)

        logger.info("Write the Output of Analysis3 under %s ", self.output_path + "analysis3")
        top_5_makes_df.coalesce(1).write.mode('overwrite').option("header", "true").csv(self.output_path + "analysis3")

        return top_5_makes_df