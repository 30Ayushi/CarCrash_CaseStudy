import logging
import pyspark.sql.functions as f
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Analysis6:
    """
    Determine Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death.

    Attributes:
        spark (SparkSession): Spark session object.
        data_dict (dict): dictionary that contains dataframes.
        output_path (str): path where the output CSV file will be saved.

    Methods:
        run(): Executes the analysis and saves the results in a CSV file.
    """
    def __init__(self, spark, data_dict, output_path):
        """
        Initializes the Analysis6 class

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
        Execute and Save the Analysis of Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
 
        Returns:
            pyspark.sql.dataframe.DataFrame: 
        """
        logger.info ("Fetch the primary_person dataset")
        primary_person_df = self.data_dict['primary_person']

        logger.info("Fetch the units dataset")
        units_df = self.data_dict['units']

        logger.info("Add new column in primary_person which will have all the injures and death")
        pp_new_col_df = primary_person_df.withColumn("SUM_INJRY_DEATH", f.col("INCAP_INJRY_CNT") + f.col("NONINCAP_INJRY_CNT") + f.col("POSS_INJRY_CNT") + f.col("UNKN_INJRY_CNT") + f.col("DEATH_CNT"))

        logger.info("Join New primary_person dataset with units dataset")
        combined_df = units_df.join(pp_new_col_df, ["CRASH_ID", "UNIT_NBR"])

        logger.info("Group the dataset based on VEH_MAKE_ID")
        injuries_df = combined_df.groupBy("VEH_MAKE_ID").agg(f.sum("SUM_INJRY_DEATH").alias("SUM_INJRY_DEATH"))

        logger.info("Apply Rank on total of death and injury")
        windowSpec = Window.orderBy(f.col("SUM_INJRY_DEATH").desc())
        rank_df = injuries_df.withColumn("rank", f.dense_rank().over(windowSpec))

        logger.info("Filter the records for 3rd to 5th rank")
        final_df = rank_df.filter((f.col("rank") >= 3) & (f.col("rank") <= 5))

        logger.info("Write the Output of analayis6 under %s ", self.output_path + "analysis6")
        final_df.coalesce(1).write.mode('overwrite').option("header", "true").csv(self.output_path + "analysis6")

        return final_df
