# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

"""
MM Financial Reconciliation Prototype

This pipeline example implements the following steps:

- Ingest a tabular datasets as a CSV files from local or GCS source
- Apply individual transforms to each dataset : add calculated fields, enrich with reference data
- Join to other datasets using compound primary keys added in previous steps
- Perform aggregations and calculations on quantity and value variance
- Write result sets to BigQuery table
- Optionally output results as CSV files in GCS bucket
"""

import argparse
import logging
import apache_beam as beam
from apache_beam.io.textio import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from modules.Names import Names


########################################################### 
# 
#              MAIN PIPELINE ROUTINE
# 
###########################################################

def run(argv=None):
    """Main pipeline routine"""

    #  Define pipeline arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--bulk',
        required=True,
        dest='is_bulk',
        help='Sample boolean parameter'
    ),
    parser.add_argument(
        '--str-date',
        required=False,
        default='2024-09-21',
        dest='str_date',
        help='Sample string date optional parameter'
    )        
     
    ########################################################### 
    # 
    #         SET PIPELINE OPTIONS & ARGUMENTS
    # 
    ###########################################################

    known_args, pipeline_args = parser.parse_known_args(argv)
     
    # Set pipeline options
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True, streaming=False)
    pipeline_options_dict = pipeline_options.get_all_options()

    # Define GCP project ID from pipeline options
    gcp_project_id = pipeline_options_dict[Names.GCP_PROJ_KEY]

    ########################################################### 
    # 
    #              EXECUTE PIPELINE
    # 
    ###########################################################

    with beam.Pipeline(options=pipeline_options) as p:

        _ = (
            p 
            | 'Read data' >> ReadFromText("gs://mm-sample-data-57647c46/mm-fin-rec/input/depots/depots.csv", skip_header_lines=1)
            | 'Dump data' >> beam.Map(logging.info)
        ) 


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

# fmt: on  