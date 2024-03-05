import logging
import pyspark.sql.functions as f
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Analysis10:
    """
    Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, 
    used top 10 used vehicle colours and 
    has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

    Attributes:
        spark (SparkSession): Spark session object.
        data_dict (dict): dictionary that contains dataframes.
        output_path (str): path where the output CSV file will be saved.

    Methods:
        run(): Executes the analysis and saves the results in a CSV file.
    """

    def __init__(self, spark, data_dict, output_path):
        """
        Initializes the Analysis10 class

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
        units_df = self.data_dict['units']
        primary_person_df = self.data_dict['primary_person']
        charges_df = self.data_dict['charges']
        
        licensed_drivers_df = primary_person_df.filter(f.col("DRVR_LIC_TYPE_ID").isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])).select("CRASH_ID").distinct()

        speeding_charges_df = charges_df.filter(f.col('CHARGE').contains('SPEED')).select("CRASH_ID").distinct()

        window_spec_top_25_st = Window.orderBy(f.col('CRASH_ID_COUNT').desc())

        top_25_states_df = units_df.join(primary_person_df, "CRASH_ID")\
                .groupBy("VEH_LIC_STATE_ID").agg(f.countDistinct("CRASH_ID").alias("CRASH_ID_COUNT"))\
                .withColumn('RANK', f.dense_rank().over(window_spec_top_25_st))\
                .filter(f.col('RANK')<= 25).select("VEH_LIC_STATE_ID")

        top_10_color_df =  units_df.filter(f.col('VEH_COLOR_ID') != 'NA')\
                            .groupBy(f.col('VEH_COLOR_ID')).agg(f.countDistinct(f.col('CRASH_ID')).alias('CRASH_ID_COUNT'))\
                            .orderBy(f.col("CRASH_ID_COUNT").desc()).limit(10)

        top_25_states_df2 = units_df.join(top_25_states_df, 'VEH_LIC_STATE_ID', 'inner')\
                .join(top_10_color_df , 'VEH_COLOR_ID','inner')\
                .join(licensed_drivers_df , 'CRASH_ID', "inner")\
                .join(speeding_charges_df , 'CRASH_ID', "inner")\
                .select(f.col('CRASH_ID'), f.col('VEH_MAKE_ID'))

        win_spec_top_5_veh_mkr = Window.orderBy(f.col('CRASH_ID_COUNT').desc())
        final_dataset = top_25_states_df2.groupBy(f.col('VEH_MAKE_ID')).agg(f.countDistinct('CRASH_ID').alias('CRASH_ID_COUNT'))\
                        .withColumn('rank', f.dense_rank().over(win_spec_top_5_veh_mkr))\
                        .where(f.col('rank') <= 5 )\
                        .select(f.col('VEH_MAKE_ID'))

        logger.info("Write the Output of Analysis10 under %s ", self.output_path + "analysis10")
        final_dataset.coalesce(1).write.mode('overwrite').option("header", "true").csv(self.output_path + "analysis10")

        return final_dataset