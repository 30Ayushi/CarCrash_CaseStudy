import logging
import pyspark.sql.functions as f
from pyspark.sql import Row

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Analysis9:
    """
    Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance.
    Attributes:
        spark (SparkSession): Spark session object.
        data_dict (dict): dictionary that contains dataframes.
        output_path (str): path where the output CSV file will be saved.

    Methods:
        run(): Executes the analysis and saves the results in a CSV file.
    """
    def __init__(self, spark, data_dict, output_path):
        """
        Initializes the Analysis9 class

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
        Execute and Save the Analysis 
 
        Returns:
            pyspark.sql.dataframe.DataFrame: 
        """
        damages_df = self.data_dict['damages']
        units_df = self.data_dict['units']
        
        logger.info("Filter the non damage property from damages dateset")
        non_damages_df = damages_df.filter(f.col("DAMAGED_PROPERTY").isNull()).select("CRASH_ID").distinct()

        filtered_unit_df = units_df.filter((f.col("VEH_DMAG_SCL_1_ID").isin(["DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST"])) | 
                                                                (f.col("VEH_DMAG_SCL_2_ID").isin(["DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST"])))\
                                                        .filter(f.col("FIN_RESP_PROOF_ID").isNotNull())

        joined_df = non_damages_df.join(filtered_unit_df, "CRASH_ID", "inner")

        crash_count = joined_df.select("CRASH_ID").distinct().count()
        count_df = self.spark.createDataFrame([Row(count=crash_count)])

        logger.info("Write the Output of Analysis9 under %s ", self.output_path + "analysis9")
        count_df.coalesce(1).write.mode('overwrite').option("header", "true").csv(self.output_path + "analysis9")
        return crash_count