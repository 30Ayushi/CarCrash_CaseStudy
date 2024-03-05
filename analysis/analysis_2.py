import logging
import pyspark.sql.functions as f
from pyspark.sql import Row

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Analysis2:
    """
    Determine no of two wheelers are booked for crashes

    Attributes:
        spark (SparkSession): Spark session object.
        data_dict (dict): dictionary that contains dataframes.
        output_path (str): path where the output CSV file will be saved.

    Methods:
        run(): Executes the analysis and saves the results in a CSV file.
    """
    def __init__(self, spark, data_dict, output_path):
        """
        Initializes the Analysis2 class

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
        Execute and Save the Analysis for no of two wheelers are booked for crashes
 
        Returns:
            pyspark.sql.dataframe.DataFrame:
        """
        units_df = self.data_dict['units']
        two_wheelers_df = units_df.filter(f.col("VEH_BODY_STYL_ID") == "MOTORCYCLE")

        # Count the distinct crashes involving two-wheelers
        crash_count = two_wheelers_df.select(f.countDistinct("CRASH_ID").alias("CRASH_COUNT"))
        
        logger.info("Write the Output of analayis2 under %s ", self.output_path + "analysis2")
        count_df.coalesce(1).write.mode('overwrite').option("header", "true").csv(self.output_path + "analysis2")
        return crash_count
