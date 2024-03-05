import argparse
import json
import logging
from pyspark.sql import SparkSession
import sys
sys.path.append('/workspaces/CarCrash_CaseStudy')

from data.input_reader import InputReader
from analysis.analysis_1 import Analysis1
from analysis.analysis_2 import Analysis2
from analysis.analysis_3 import Analysis3
from analysis.analysis_4 import Analysis4
from analysis.analysis_5 import Analysis5
from analysis.analysis_6 import Analysis6
from analysis.analysis_7 import Analysis7
from analysis.analysis_8 import Analysis8
from analysis.analysis_9 import Analysis9
from analysis.analysis_10 import Analysis10

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

def load_config(config_path):
    """
    Loads the input from the config file
    Parameters:
        config_path (str): path of config.json file.
    Returns:
        dict: contains the json value.
    """
    with open(config_path, 'r') as file:
        return json.load(file)

def main():
    """
    Executes all analysis
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True, help='Path to the config file')
    args = parser.parse_args()

    config = load_config(args.config)
    input_path = config['publish']['input_path']
    output_path = config['publish']['output_path']

    logger.info("Input Path .... %s ", input_path)

    # Creating Spark Session
    spark = SparkSession.builder.appName("Vehicle Crash Analytics").getOrCreate()

    reader_obj = InputReader(spark, input_path)
    dataset_dict = reader_obj.load_all_data()

    analysis1_obj = Analysis1(spark, dataset_dict, output_path)
    analysis1_obj.run()

    analysis2_obj = Analysis2(spark, dataset_dict, output_path)
    analysis2_obj.run()

    analysis3_obj = Analysis3(spark, dataset_dict, output_path)
    analysis3_obj.run()

    analysis4_obj = Analysis4(spark, dataset_dict, output_path)
    analysis4_obj.run()

    analysis5_obj = Analysis5(spark, dataset_dict, output_path)
    analysis5_obj.run()

    analysis6_obj = Analysis6(spark, dataset_dict, output_path)
    analysis6_obj.run()

    analysis7_obj = Analysis7(spark, dataset_dict, output_path)
    analysis7_obj.run()

    analysis8_obj = Analysis8(spark, dataset_dict, output_path)
    analysis8_obj.run()

    analysis9_obj = Analysis9(spark, dataset_dict, output_path)
    analysis9_obj.run()

    analysis10_obj = Analysis10(spark, dataset_dict, output_path)
    analysis10_obj.run()

    spark.stop()

if __name__ == "__main__":
    main()