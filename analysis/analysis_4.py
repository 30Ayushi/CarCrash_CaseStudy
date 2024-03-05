import logging
import pyspark.sql.functions as f
from pyspark.sql import Row

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Analysis4:
    """
    Determine number of Vehicles with driver having valid licences involved in hit and run.

    Attributes:
        spark (SparkSession): Spark session object.
        data_dict (dict): dictionary that contains dataframes.
        output_path (str): path where the output CSV file will be saved.

    Methods:
        run(): Executes the analysis and saves the results in a CSV file.
    """

    def __init__(self, spark, data_dict, output_path):
        """
        Initializes the Analysis4 class

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
        Execute and Save the Analysis for those vehicles who are involed in hit & run cases with the drivers having the valid licenses.
 
        Returns:
            pyspark.sql.dataframe.DataFrame: 
        """
        logger.info ("Fetch the primary_person dataset")
        primary_person_df = self.data_dict['primary_person']

        logger.info("Fetch the units dataset")
        units_df = self.data_dict['units']

        logger.info("Filter the primary_person dataset drivers with valid licenses in primary person")
        valid_license_df = primary_person_df.filter((f.col('PRSN_TYPE_ID').isin(['DRIVER OF MOTORCYCLE TYPE VEHICLE', 'DRIVER'])) &
                                                    (f.col('DRVR_LIC_TYPE_ID').isin(['COMMERCIAL DRIVER LIC.', 'DRVR_LIC_TYPE_ID'])))

        logger.info("Filter the unit dataset based on hit & run case.")
        hit_and_run_df = units_df.filter(f.col("VEH_HNR_FL") == "Y")

        logger.info("Join the filtered primary_person and units")
        valid_licenses_hit_and_run_df = valid_license_df.join(hit_and_run_df, 
                                                                (valid_license_df.CRASH_ID == hit_and_run_df.CRASH_ID) & 
                                                                (valid_license_df.UNIT_NBR == hit_and_run_df.UNIT_NBR), 'inner'). \
                                                                select(valid_license_df.CRASH_ID, valid_license_df.UNIT_NBR)

        logger.info("Count the distinct number of vehicles")
        num_vehicles = valid_licenses_hit_and_run_df.select("CRASH_ID", "UNIT_NBR").distinct().count()
        count_df = self.spark.createDataFrame([Row(count=num_vehicles)])

        logger.info("Write the Output of analayis4 under %s ", self.output_path + "analysis4")
        count_df.coalesce(1).write.mode('overwrite').option("header", "true").csv(self.output_path + "analysis4")

        return count_df