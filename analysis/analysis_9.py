import logging
import pyspark.sql.functions as f

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

        filtered_unit_df = units_df.select("VEH_DMAG_SCL_1_ID", "VEH_DMAG_SCL_2_ID", "FIN_RESP_TYPE_ID", "FIN_RESP_PROOF_ID", "CRASH_ID")\
                                    .withColumn('VEH_DMG_LVL_1',f.regexp_extract(f.col('VEH_DMAG_SCL_1_ID'), r'(\d+)', 1).cast('bigint'))\
                                    .withColumn('VEH_DMG_LVL_2',f.regexp_extract(f.col('VEH_DMAG_SCL_2_ID'), r'(\d+)', 1).cast('bigint'))\
                                    .filter((~f.col('FIN_RESP_PROOF_ID').isin('NA','NR')) & \
                                            ((f.col('VEH_DMG_LVL_1') > 4) | (f.col('VEH_DMG_LVL_2') > 4)) & \
                                            f.col('FIN_RESP_TYPE_ID').contains('INSURANCE'))

        non_damages_df = filtered_unit_df.join(damages_df, "CRASH_ID", "leftanti")

        final_dataset = non_damages_df.select(f.countDistinct(f.col('CRASH_ID')).alias('CRASH_ID_COUNT'))

        logger.info("Write the Output of Analysis9 under %s ", self.output_path + "analysis9")
        final_dataset.coalesce(1).write.mode('overwrite').option("header", "true").csv(self.output_path + "analysis9")

        return final_dataset
