import logging
import pyspark.sql.functions as f

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

class Analysis1:
    """
    Determine the number of crashes (accidents) in which number of males killed are greater than 2

    Attributes:
        spark (SparkSession): Spark session object.
        data_dict (dict): dictionary that contains dataframes.
        output_path (str): path where the output CSV file will be saved.

    Methods:
        run(): Executes the analysis and saves the results in a CSV file.
    """
    def __init__(self, spark, data_dict, output_path):
        """
        Initializes the Analysis1 class

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
        Execute and Save the Analysis for number of crashes (accidents) in which number of males killed are greater than 2.
 
        Returns:
            pyspark.sql.dataframe.DataFrame:        
        """
        logger.info("Fetch the data from primary_person dataset")
        primary_person_df = self.data_dict['primary_person']

        logger.info("Filter the datset based on gender and injury sev ")
        killed_male_df = primary_person_df.filter((f.col("PRSN_GNDR_ID") == "MALE") & (f.col("PRSN_INJRY_SEV_ID") == "KILLED"))

        logger.info("Count the no of crashes")
        count_df = killed_male_df.groupBy("CRASH_ID").count()

        logger.info("Filter the data where count of crashes is greater than 2")
        final_dataset = count_df.filter(f.col("count") > 2).select("count")

        logger.info("Write the Output of analayis1 under %s ", self.output_path + "analysis1")
        final_dataset.coalesce(1).write.mode('overwrite').option("header", "true").csv(self.output_path + "analysis1")

        
   