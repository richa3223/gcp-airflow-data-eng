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
from modules.BigQueryUtils import BigQueryUtils
from modules.CsvFileUtils import CsvFileUtils
from modules.Filters import Filters
from modules.FinRecData import FinRecData
from modules.Mappers import Mappers
from modules.Names import Names
from modules.Parsers import Parsers
from modules.Pricing import Pricing
from modules.SummaryTotal import SummaryTotal
from modules.Variance import Variance
from modules.Transforms import (
    AggregateVariance, 
    CalculateVarianceTotals, 
    CollectionAsDecodeDict, 
    CsvToDict, 
    DatasetIngestAndEnrich, 
    LeftJoin, 
    LoadIntoBigQuery,
    NFSIDataEnrichAndTransform,
    SideInputAsDecodeDict,
    write_data_as_csv
)

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
        '--pkrd',
        required=True,
        dest='pkrd',
        help='Path to PKRD input dataset e.g. local file or GCS path'
    ),
    parser.add_argument(
        '--sales',
        required=True,
        dest='sales_order',
        help='Path to Sales Order input dataset e.g. local file or GCS path'
    ),
    parser.add_argument(
        '--pricing',
        required=True,
        dest='pricing',
        help='Path to Transfer Pricing input dataset e.g. local file or GCS path'
    ),   
    parser.add_argument(
        '--depot',
        required=True,
        dest='depot',
        help='Path to Depot reference dataset e.g. local file or GCS path'
    ),        
    parser.add_argument(
        '--fresh',
        required=True,
        dest='fresh',
        help='Path to NFSI Fresh input dataset e.g. local file or GCS path'
    ),  
    parser.add_argument(
        '--frozen',
        required=True,
        dest='frozen',
        help='Path to NFSI Frozen input dataset e.g. local file or GCS path'
    ),  
    parser.add_argument(
        '--non-nfsi',
        required=True,
        dest='non_nfsi',
        help='Path to Non-NFSI input dataset e.g. local file or GCS path'
    ),   
    parser.add_argument(
        '--start-date',
        required=False,
        default=None,
        dest='start_date',
        help='Optional date range start'
    ),   
    parser.add_argument(
        '--end-date',
        required=False,
        default=None,
        dest='end_date',
        help='Optional date range end'
    ),     
    parser.add_argument(
        '--effective-date',
        required=False,
        default=None,
        dest='effective_date',
        help='Effective date of report'
    ),           
    parser.add_argument(
        '--bq-output',
        required=False,
        default=False,
        dest='bq_output',
        help='Flag indicating whether to write output to BigQuery'
    ),  
    parser.add_argument(
        '--file-output',
        required=False,
        default=False,
        dest='file_output',
        help='Flag indicating whether to output processed files'
    ),    
    parser.add_argument(
        '--output-dir',
        required=False,
        dest='output',
        help='Path to directory for output files'
    )

    ########################################################### 
    # 
    #         SET PIPELINE OPTIONS & ARGUMENTS
    # 
    ###########################################################

    known_args, pipeline_args = parser.parse_known_args(argv)
            
    # Format report date range and effective date if provided
    filter_dates = Parsers.filter_dates(known_args.start_date, known_args.end_date)

    # Capture effective date from parameter input or default to current date
    effective_date = Parsers.effective_date(known_args.effective_date)
    
    # Parse options flags
    output_to_bq = Parsers.str_to_bool(str(known_args.bq_output))
    output_to_file = Parsers.str_to_bool(str(known_args.file_output))
     
    # Set pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options_dict = pipeline_options.get_all_options()

    # Define GCP project ID from pipeline options
    gcp_project_id = pipeline_options_dict[Names.GCP_PROJ_KEY]

    # Extract CSV column names from input datasets
    pkrd_col_names = CsvFileUtils.csv_column_names(known_args.pkrd)
    sales_order_col_names = CsvFileUtils.csv_column_names(known_args.sales_order)
    pricing_col_names = CsvFileUtils.csv_column_names(known_args.pricing)
    fresh_col_names = CsvFileUtils.csv_column_names(known_args.fresh)
    frozen_col_names = CsvFileUtils.csv_column_names(known_args.frozen)
    non_nfsi_col_names = CsvFileUtils.csv_column_names(known_args.non_nfsi)
    depot_col_names = [Names.DEPOT_ID, Names.DEPOT_NAME, Names.DEPOT_CATEGORY]

    ########################################################### 
    # 
    #              EXECUTE PIPELINE
    # 
    ###########################################################

    with beam.Pipeline(options=pipeline_options) as p:

        # Ingest Depot reference data as side-input for dataset enrichment 
        depots_decode = (
            p
            | 'Depots side input' >> SideInputAsDecodeDict(known_args.depot,
                                                     depot_col_names,
                                                     Names.TYPE_DEPOTS,
                                                     Names.DEPOT_ID)            
        )

        # Ingest Transfer Pricing data
        pricing_data = (
            p
            | 'Read Pricing CSV' >> ReadFromText(known_args.pricing, skip_header_lines=1)
            | 'Pricing to dictionary' >> beam.ParDo(CsvToDict(), pricing_col_names)
        )

        # Create a keyed decode side-input for Transfer Pricing
        pricing_decode = (
            pricing_data
            | 'Pricing as look-up' >> beam.CombineGlobally(CollectionAsDecodeDict(Names.TP_SKU))            
        )   
         
        # Transform Transfer Pricing to data model 
        pricing = (
            pricing_data
            | 'Pricing to model' >> beam.Map(Pricing.from_dataset).with_output_types(Pricing)
        ) 

        # Enrich and transform sales order data to join to PKRD 
        sales = (
            p
            | 'Read Sales Order CSV' >> ReadFromText(known_args.sales_order, skip_header_lines=1)
            | 'Sales to dictionary' >> beam.ParDo(CsvToDict(), sales_order_col_names)
            | 'Add Sales computed fields' >> beam.Map(Mappers.add_computed_fields, Names.TYPE_SALES)
        )

        sales_pkrd_extract = (
            sales
            | 'Sales PKRD extract' >> beam.Map(Mappers.subset_for_join, Names.SKU_MO, Names.SALES_SLICE)
        )

        # Enrich and transform PKRD data to join to Sales

        sales_nfsi_extract = (
            sales
            | 'Sales NFSI extract' >> beam.Map(Mappers.subset_for_join, Names.ORDER_ID, Names.SALES_SLICE)
        )

        # Enrich and transform PKRD data to join to Sales
        pkrd_sales_extract = (
            p
            | 'Ingest and enrich PKRD'
            >> DatasetIngestAndEnrich(known_args.pkrd,
                                      pkrd_col_names,
                                      Names.TYPE_PKRD,
                                      depots_decode)
            | 'Add pricing to PKRD'
            >> beam.Map(Mappers.add_pricing_data_fields, beam.pvalue.AsSingleton(pricing_decode))
            | 'PKRD sales extract'
            >> beam.Map(Mappers.subset_for_join, Names.SKU_MO)
        )

        # Join PKRD to Sales to add NFSI Order Number. Transform rows to FinRecData model
        # Note : input data is filtered to remove any records with move orders starting 'SS'
        pkrd = ((
            {Names.TYPE_PKRD: pkrd_sales_extract, Names.TYPE_SALES: sales_pkrd_extract}
            )
            | 'Join PKRD to Sales'
            >> LeftJoin(Names.TYPE_PKRD, Names.TYPE_SALES)
            | 'PKRD to FinRecData'
            >> beam.Map(FinRecData.from_pkrd).with_output_types(FinRecData)
            | 'Filter PKRD moveorders'
            >> beam.Filter(lambda row: Filters.filter_exclude_moveorder_prefix(row, prefix='SS')).with_input_types(FinRecData)
            | 'Filter PKRD depots'
            >> beam.Filter(lambda row: Filters.filter_exclude_depot_id(row, id='CSL')).with_input_types(FinRecData)
        )

        # Enrich NFSI Fresh data with computed fields and joins to Sales and PKRD. Transform to FinRecData model
        fresh = (
            p
            | 'Enrich and transform Fresh'
            >> NFSIDataEnrichAndTransform(known_args.fresh, 
                                          fresh_col_names,
                                          Names.TYPE_FRESH,
                                          depots_decode,
                                          sales_nfsi_extract)                                                                                                                               
            | 'Fresh to FinRecData'
            >> beam.Map(FinRecData.from_fresh).with_output_types(FinRecData)
        )
     
        # Enrich NFSI Frozen data with computed fields and joins to Sales and PKRD. Transform to FinRecData model
        frozen = (
            p
            | 'Enrich and transform Frozen'
            >> NFSIDataEnrichAndTransform(known_args.frozen, 
                                          frozen_col_names,
                                          Names.TYPE_FROZEN,
                                          depots_decode,
                                          sales_nfsi_extract)
            | 'Frozen to FinRecData'
            >> beam.Map(FinRecData.from_frozen).with_output_types(FinRecData)            
        )

        # Enrich Non-NFSI data with computed fields and joins to Sales and PKRD. Transform to FinRecData model
        non_nfsi = (
            p
            | 'Enrich and transform Non-NFSI'
            >> NFSIDataEnrichAndTransform(known_args.non_nfsi,
                                          non_nfsi_col_names,
                                          Names.TYPE_NON_NFSI,
                                          depots_decode,
                                          sales_nfsi_extract)
            | 'Non-NFSI to FinRecData'
            >> beam.Map(FinRecData.from_non_nfsi).with_output_types(FinRecData)
            | 'Filter Non-NSFI depot category'
            >> beam.Filter(lambda row: Filters.filter_by_category(row, category=Names.TYPE_NON_NFSI)).with_input_types(FinRecData)
        )   

        # Flatten PKRD, Fresh, Frozen and Non-NFSI into a single PCollection of FinRecData models
        fin_rec_data = (
            (pkrd, fresh, frozen, non_nfsi)
            | 'Flatten pcolls' >> beam.Flatten()
        )

        # Variance aggregation by Depot by SKU for Frozen
        var_by_depot_sku_frozen = (
            fin_rec_data
            | 'Filter combined data for date range'
            >> beam.Filter(lambda row: Filters.filter_for_dates(row, dates=filter_dates)).with_input_types(FinRecData)
            | 'Variance by Frozen, Depot, SKU'
            >> AggregateVariance(Names.TYPE_FROZEN, ['depot_id', 'depot_category', 'depot_name', 'sku'])   
            | 'Convert Depot, SKU to model'
            >> beam.Map(lambda r: Variance.from_result(r, var_type=Names.FROZEN_DEPOT_SKU_VAR)).with_output_types(Variance)
        )

        # Variance aggregation by SKU for Fresh
        var_by_sku_fresh = (
            fin_rec_data  
            | 'Variance by Fresh, SKU'
            >> AggregateVariance(Names.TYPE_FRESH, ['depot_category','sku'])
            | 'Convert Fresh, SKU to model'
            >> beam.Map(lambda r: Variance.from_result(r, var_type=Names.FRESH_SKU_VAR)).with_output_types(Variance)     
        )

        # Variance aggregation by SKU for Frozen
        var_by_sku_frozen = (
            fin_rec_data
            | 'Variance by Frozen, SKU'
            >> AggregateVariance(Names.TYPE_FROZEN, ['depot_category','sku'])             
            | 'Convert Frozen, SKU to model'
            >> beam.Map(lambda r: Variance.from_result(r, var_type=Names.FROZEN_SKU_VAR)).with_output_types(Variance)
        )

        # Variance aggregation by Moveorder, Fresh
        var_by_mo_fresh = (
            fin_rec_data
            | 'Variance by Fresh, Moveorder'
            >> AggregateVariance(Names.TYPE_FRESH, ['depot_category','moveorder_short']) 
            | 'Convert Fresh, Moveorder to model'
            >> beam.Map(lambda r: Variance.from_result(r, var_type=Names.FRESH_MO_VAR)).with_output_types(Variance)             
        )

        # Calculate summary variance, PTD and sales percentage reporting values including ex-GIT values
        fresh_summary_totals = (
            var_by_mo_fresh
            | 'Calculate summary for Fresh' >> CalculateVarianceTotals(Names.TYPE_FRESH)   
            | 'Convert Fresh result to summary model'
            >> beam.Map(lambda r: SummaryTotal.from_result(r)).with_output_types(SummaryTotal)                                                                                             
        )

        # Variance aggregation by Moveorder, Non-NFSI
        var_by_mo_non_nfsi = (
            fin_rec_data
            | 'Variance by Non-NFSI, Moveorder'
            >> AggregateVariance(Names.TYPE_NON_NFSI, ['depot_category','moveorder_short'])
            | 'Convert Non-NFSI, Moveorder to model'
            >> beam.Map(lambda r: Variance.from_result(r, var_type=Names.NON_NFSI_MO_VAR)).with_output_types(Variance)  
        )   

        # Calculate summary variance, PTD and sales percentage reporting values including ex-GIT values
        non_nfsi_summary_totals = (
            var_by_mo_non_nfsi
            | 'Calculate summary for Non-NFSI' >> CalculateVarianceTotals(Names.TYPE_NON_NFSI)   
            | 'Convert Non-NFSI result to summary model'
            >> beam.Map(lambda r: SummaryTotal.from_result(r)).with_output_types(SummaryTotal)                                                                                      
        )

        # Variance aggregation by Depot, Date, Frozen (Goods in transit ?)
        var_by_depot_date_frozen = (
            fin_rec_data
            | 'Variance for Frozen, Depot, Date'
            >> AggregateVariance(Names.TYPE_FROZEN, ['depot_category', 'depot_id', 'depot_name', 'record_date'])
            | 'Convert Depot, Date, Frozen to model'
            >> beam.Map(lambda r: Variance.from_result(r, var_type=Names.FROZEN_DEPOT_DATE_VAR)).with_output_types(Variance)                                       
        ) 

        # Flatten variance results into a single PCollection
        variance_datasets = (
            [
                var_by_depot_sku_frozen,
                var_by_sku_fresh,
                var_by_sku_frozen,
                var_by_mo_fresh,
                var_by_mo_non_nfsi,
                var_by_depot_date_frozen
            ]
            | 'Flatten variance datasets into single collection' >> beam.Flatten()
        )

        # Calculate summary variance, PTD and sales percentage reporting values including ex-GIT values
        frozen_summary_totals = (
            var_by_depot_date_frozen
            | 'Calculate summary for Frozen'
            >> CalculateVarianceTotals(Names.TYPE_FROZEN)   
            | 'Convert Frozen result to summary model'
            >> beam.Map(lambda r: SummaryTotal.from_result(r)).with_output_types(SummaryTotal)                                                                                                 
        )        

        # Flatten summary totals into single PCollection
        summary_totals = (
            (fresh_summary_totals, non_nfsi_summary_totals, frozen_summary_totals)
            | 'Flatten summaries into single collection' >> beam.Flatten()
        )  

        # Calculate report level summary total values
        report_totals = (
            summary_totals
            | 'Calculate grand totals'
            >> beam.GroupBy('report_type')
                    .aggregate_field('pkrd_quantity_sum', sum, 'sum_pkrd_quantity')  
                    .aggregate_field('pkrd_value_tp_sum', sum, 'sum_pkrd_value_tp')
                    .aggregate_field('nfsi_quantity_sum', sum, 'sum_nfsi_quantity')
                    .aggregate_field('nfsi_value_sum', sum, 'sum_nfsi_value')
                    .aggregate_field('quantity_variance_sum', sum, 'sum_quantity_variance')
                    .aggregate_field('value_variance_sum', sum, 'sum_value_variance_tp') 
                    .aggregate_field('git_quantity_sum', sum, 'sum_git_quantity')
                    .aggregate_field('git_value_sum', sum, 'sum_git_value') 
            | 'Convert grand totals to summary model'
            >> beam.Map(lambda r: SummaryTotal.from_result(r)).with_output_types(SummaryTotal)                                                    
        )

        # Flatten summary and grand total collections
        summary_report = (
            (summary_totals, report_totals)
            | 'Flatten summary and grand totals' >> beam.Flatten()
        )

        ########################################################### 
        # 
        #   OPTIONALLY LOAD PROCESSED RESULT SETS INTO BIGQUERY
        # 
        ###########################################################

        if output_to_bq == True:
            
            metadata_fields = BigQueryUtils.metadata_fields()
            metadata_plus_date = BigQueryUtils.metadata_plus_date(metadata_fields, effective_date)
            metadata_plus_valid_from = BigQueryUtils.metadata_plus_valid_from(metadata_fields)

            _ = (
                pricing
                | 'Format Pricing for BigQuery'
                >> beam.Map(lambda row: row.bigquery_dict(metadata_fields)).with_input_types(Pricing)        
                | 'Write Pricing to BigQuery'
                >> LoadIntoBigQuery(Names.TABLE_FIN_REC_PRICING, Names.DATASET_INTERNAL, gcp_project_id)
            )

            _ = (
                fin_rec_data
                | 'Format FinRecData for BigQuery'
                >> beam.Map(lambda row: row.bigquery_dict(metadata_plus_valid_from)).with_input_types(FinRecData)
                | 'Write FinRecData to BigQuery'
                >> LoadIntoBigQuery(Names.TABLE_FIN_REC_DATA, Names.DATASET_INTERNAL, gcp_project_id)
            )

            _ = (
                variance_datasets
                | 'Format Variance for BigQuery'
                >> beam.Map(lambda row: row.bigquery_dict(metadata_plus_date)).with_input_types(Variance)
                | 'Write Variance to BigQuery'
                >> LoadIntoBigQuery(Names.TABLE_FIN_REC_VAR, Names.DATASET_INTERNAL, gcp_project_id)
            )

            _ = (
                summary_report
                | 'Format Summary Report for BigQuery'
                >> beam.Map(lambda row: row.bigquery_dict(metadata_plus_date)).with_input_types(SummaryTotal)        
                | 'Write Summary Report to BigQuery'
                >> LoadIntoBigQuery(Names.TABLE_FIN_REC_SUMMARY, Names.DATASET_INTERNAL, gcp_project_id)                
            )

        ########################################################### 
        # 
        #   OPTIONALLY OUTPUT PROCESSED RESULT SETS AS CSV
        # 
        ###########################################################            

        if output_to_file == True:
            # Output enriched and flattened source dataset
            write_data_as_csv(fin_rec_data, f'{known_args.output}/fin-rec-data', 'finrecdata')

            # Output Frozen grouped by Depot and SKU
            write_data_as_csv(var_by_depot_sku_frozen, f'{known_args.output}/frozen', 'frozen-var-depot-sku')

            # Output Fresh grouped by SKU   
            write_data_as_csv(var_by_sku_fresh, f'{known_args.output}/fresh', 'fresh-var-sku')

            # Output Frozen grouped by SKU
            write_data_as_csv(var_by_sku_frozen, f'{known_args.output}/frozen', 'frozen-var-sku')

            # Output Fresh grouped by Moveorder
            write_data_as_csv(var_by_mo_fresh, f'{known_args.output}/fresh', 'fresh-var-mo')

            # Output Non-NFSI grouped by Moveorder
            write_data_as_csv(var_by_mo_non_nfsi, f'{known_args.output}/non-nfsi', 'non-nfsi-var-mo')

            # Output Frozen grouped by Depot, Date
            write_data_as_csv(var_by_depot_date_frozen, f'{known_args.output}/frozen', 'frozen-var-depot-date')

            # Output report grand totals
            write_data_as_csv(summary_report, f'{known_args.output}/report-totals', 'fin-rec-report-totals') 

            # _ = (
            #     fin_rec_data
            #     | 'Write enriched base dataset'
            #     >> WriteDataAsCsv(f'{known_args.output}/fin-rec-data', 'finrecdata', FinRecData._fields)
            # ) 

            # # Output Frozen grouped by Depot and SKU
            # _ = (
            #     var_by_depot_sku_frozen
            #     | 'Write Frozen by Depot, SKU'
            #     >> WriteDataAsCsv(f'{known_args.output}/frozen', 'frozen-var-depot-sku')
            # )  

            # # Output Fresh grouped by SKU
            # _ = (
            #     var_by_sku_fresh
            #     | 'Write Fresh by SKU'
            #     >> WriteDataAsCsv(f'{known_args.output}/fresh', 'fresh-var-sku')
            # ) 

            # # Output Frozen grouped by SKU
            # _ = (
            #     var_by_sku_frozen
            #     | 'Write Frozen by SKU'
            #     >> WriteDataAsCsv(f'{known_args.output}/frozen', 'frozen-var-sku')
            # )   

            # # Output Fresh grouped by Moveorder
            # _ = (
            #     var_by_mo_fresh
            #     | 'Write Fresh by Moveorder'
            #     >> WriteDataAsCsv(f'{known_args.output}/fresh', 'fresh-var-mo')
            # )   

            # # Output Non-NFSI grouped by Moveorder
            # _ = (
            #     var_by_mo_non_nfsi
            #     | 'Write Non-NFSI by Moveorder'
            #     >> WriteDataAsCsv(f'{known_args.output}/non-nfsi', 'non-nfsi-var-mo')
            # )       

            # # Output Frozen grouped by Depot, Date
            # _ = (
            #     var_by_depot_date_frozen
            #     | 'Write Frozen by Depot, Date'
            #     >> WriteDataAsCsv(f'{known_args.output}/frozen', 'frozen-var-depot-date')
            # )  

            # # Output report grand totals
            # _ = (
            #     summary_report
            #     | 'Write report grand totals'
            #     >> WriteDataAsCsv(f'{known_args.output}/report-totals', 'fin-rec-report-totals')
            # )                                              
        
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

# fmt: on  