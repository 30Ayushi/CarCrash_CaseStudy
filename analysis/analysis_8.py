import logging
import pyspark.sql.functions as f

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Analysis8:
    """
    Determine the top 5 zip codes with the highest number of crashes with alcohol as a contributing factor
    Attributes:
        spark (SparkSession): Spark session object.
        data_dict (dict): dictionary that contains dataframes.
        output_path (str): path where the output CSV file will be saved.

    Methods:
        run(): Executes the analysis and saves the results in a CSV file.
    """
    def __init__(self, spark, data_dict, output_path):
        """
        Initializes the Analysis8 class

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
        Execute and Save the Analysis for top 5 zip codes with the highest number of crashes where alcohol was involved.
 
        Returns:
            pyspark.sql.dataframe.DataFrame: 
        """
        logger.info ("Fetch the primary_person dataset")
        primary_person_df = self.data_dict['primary_person']

        logger.info("Fetch the units dataset")
        units_df = self.data_dict['units']

        logger.info("Filter the primary_person dataset where alcohol was involed")
        alcohol_df = primary_person_df.filter(f.col("PRSN_ALC_RSLT_ID") == "Positive")

        logger.info("Aggregate  the no of crashes by driver zip code")
        driver_zip_count =alcohol_df.groupBy("DRVR_ZIP").agg(f.count("*").alias("crash_count"))

        final_df = driver_zip_count.orderBy(f.col("crash_count").desc()).select("DRVR_ZIP").limit(5)

        logger.info("Write the Output of analayis6 under %s ", self.output_path + "analysis6")
        final_df.coalesce(1).write.mode('overwrite').option("header", "true").csv(self.output_path + "analysis8")

        return final_df