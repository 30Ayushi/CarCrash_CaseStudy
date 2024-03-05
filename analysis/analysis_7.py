import logging
import pyspark.sql.functions as f
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Analysis7:
    """
    Determine all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    Attributes:
        spark (SparkSession): Spark session object.
        data_dict (dict): dictionary that contains dataframes.
        output_path (str): path where the output CSV file will be saved.

    Methods:
        run(): Executes the analysis and saves the results in a CSV file.
    """
    def __init__(self, spark, data_dict, output_path):
        """
        Initializes the Analysis7class

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
        Execute and Save the Analysis for all the body styles involved in crashes, mention the top ethnic user group of each unique body style
 
        Returns:
            pyspark.sql.dataframe.DataFrame: 
        """
        units_df = self.data_dict['units']
        primary_person_df = self.data_dict['primary_person']
        
        logger.info("Filter the Primary_person Dataset based on PRSN_ETHNICITY_ID")
        filtered_primary_person_df = primary_person_df.filter(~(f.col("PRSN_ETHNICITY_ID")).isin('NA','UNKNOWN'))

        logger.info("Filter the units dataset based on VEH_BODY_STYL_ID")
        filtered_units_df = units_df.filter(~(f.col('VEH_BODY_STYL_ID').isin(["NA", "NOT REPORTED", "UNKNOWN"])))

        joined_df = filtered_units_df.join(filtered_primary_person_df, "CRASH_ID", "inner")\
                    .select("CRASH_ID", "VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")

        agg_df = joined_df.groupBy(f.col('VEH_BODY_STYL_ID'), f.col('PRSN_ETHNICITY_ID'))\
                            .agg(f.countDistinct(f.col('CRASH_ID')).alias('DIS_CNT_CRASH_ID')).orderBy(f.col('VEH_BODY_STYL_ID'))

        window_spec = Window.partitionBy(f.col('VEH_BODY_STYL_ID')).orderBy(f.col('DIS_CNT_CRASH_ID').desc())

        final_dataset = agg_df.withColumn("RANK", f.dense_rank().over(window_spec))\
                            .filter(f.col('rank') == 1)\
                            .select(f.col('VEH_BODY_STYL_ID'), f.col('PRSN_ETHNICITY_ID').alias('TOP_PRSN_ETHNICITY_ID'))

        logger.info("Write the Output of Analysis7 under %s ", self.output_path + "analysis7")
        final_dataset.coalesce(1).write.mode('overwrite').option("header", "true").csv(self.output_path + "analysis7")

        return final_dataset