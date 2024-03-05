class InputReader:
    """
    This class is responsible for reading the input files
    """
    def __init__(self, spark, base_path):
        self.spark = spark
        self.base_path = base_path

    def read_csv(self, file_name: str):
        """
        This function will read the csv and covert them into spark dataframe.

        Parameters:
        file_name (str): Name of the csv.

        Returns:
        pyspark.sql.dataframe.DataFrame: Spark Dataframe
        """
        file_path = self.base_path + file_name
        return self.spark.read.csv(file_path, header=True, inferSchema=True)

    def load_all_data(self):
        """
        This function is responsible to read all the input datasets.

        Parameters:
        None

        Returns:
        dict: returns a dict of the dataset.
        """
        charges_df = self.read_csv("Charges_use.csv")
        damages_df = self.read_csv("Damages_use.csv")
        endrose_df = self.read_csv("Endorse_use.csv")
        primary_person_df = self.read_csv("Primary_Person_use.csv")
        restrict_df = self.read_csv("Restrict_use.csv")
        units_df = self.read_csv("Units_use.csv")

        return {
            "charges": charges_df,
            "damages": damages_df,
            "endrose": endrose_df,
            "primary_person": primary_person_df,
            "restrict": restrict_df,
            "units":units_df
        }