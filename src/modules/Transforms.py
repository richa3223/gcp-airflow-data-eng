# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

"""
Provides a number of reusable Apache Beam composite transforms
"""

__all__ = [
    "AggregateVariance",
    "CalculateVarianceTotals",
    "CollectionAsDecodeDict",
    "CsvToDict",
    "DatasetIngestAndEnrich",
    "LeftJoin",
    "LoadIntoBigQuery"
    "NFSIDataEnrichAndTransform",
    "SideInputAsDecodeDict",
    "write_data_as_csv"
]

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.textio import ReadFromText, WriteToText, WriteToCsv
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.dataframe.io import to_csv
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from modules.CsvFileUtils import CsvFileUtils
from modules.FinRecData import FinRecData
from modules.Names import Names
from modules.Filters import Filters
from modules.Mappers import Mappers

# Transform reference data PCollection into decode dictionary
class CollectionAsDecodeDict(beam.CombineFn):
    """Transforms reference data collection into a decode value keyed dictionary"""       
    def __init__(self, key_name: str):
        # TODO(BEAM-6158): Revert the workaround once Beam can pickle super() on py3.
        # super().__init__()
        beam.CombineFn().__init__(self)            
        self._key_name = key_name

    def create_accumulator(self):
        return {}

    def add_input(self, accumulator, element):
        key = element[self._key_name]
        accumulator[key] = element
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = {}
        for a in accumulators:
            for k,v in a.items():
                merged[k] = v
        return merged

    def extract_output(self, accumulator):
        return accumulator
    
    def compact(self, accumulator):
        return accumulator  

# Transform to convert each row in ingested CSV PCollection to a dict
class CsvToDict(beam.DoFn):
    """Transforms CSV rows into dictionary"""
    def process(self, element, csv_col_names):
        yield CsvFileUtils.csv_row_as_dict(element, csv_col_names)      

# Composite transform to peform common load and enrichment transforms
class DatasetIngestAndEnrich(beam.PTransform):
    """Ingests CSV data, adds computed fields, enriches with static reference data from side input"""

    def __init__(self, data_source, cols: list[str], type: str, depots: dict):
        beam.PTransform.__init__(self)
        self._data_source = data_source
        self._cols = cols
        self._type = type
        self._cols = cols
        self._depots = depots

    def expand(self, pcoll):
        return (
            pcoll
            | 'Read {} CSV'.format(self._type) >> ReadFromText(self._data_source, skip_header_lines=1)
            | '{} to dictionary'.format(self._type) >> beam.ParDo(CsvToDict(), self._cols)
            | 'Add {} computed fields'.format(self._type) >> beam.Map(Mappers.add_computed_fields, self._type)
            | 'Add depot fields to {}'.format(self._type) >> beam.Map(Mappers.add_depot_ref_data_fields, beam.pvalue.AsSingleton(self._depots))
        )
    
# Composite transform to ingest, enrich and join to NFSI datasets to Sales data
class NFSIDataEnrichAndTransform(beam.PTransform):
    """Enriches NFSI dataset with computed fields, Sales and PKRD data via joins"""

    def __init__(self, data_source, cols: list[str], type: str, depots: dict, sales):
        beam.PTransform.__init__(self)
        self._data_source = data_source
        self._cols = cols
        self._type = type
        self._cols = cols
        self._depots = depots
        self._sales = sales

    def expand(self, pcoll):
        nfsi_sales_extract = (
            pcoll
            | 'Ingest and enrich {}'.format(self._type) >> DatasetIngestAndEnrich(self._data_source,
                                                                                    self._cols, 
                                                                                    self._type, 
                                                                                    self._depots)
            | '{} Sales extract'.format(self._type) >> beam.Map(Mappers.subset_for_join, Names.ORDER_ID)
        )

        # Join NFSI to Sales to add sku_and_moveorder using SKU and NFSI Order Number composite key
        return ((
            {self._type: nfsi_sales_extract, Names.TYPE_SALES: self._sales}
            )
            | 'Join {} to Sales'.format(self._type) >> LeftJoin(self._type, Names.TYPE_SALES)   
        )     
    
# Composite transform to load a side input dataset as a keyed decode look-up dictionary
class SideInputAsDecodeDict(beam.PTransform):
    """Ingests side input CSV dataset and transforms to a look-up dictionary"""

    def __init__(self, data_source, cols: list[str], type: str, lookup_key: str):
        beam.PTransform.__init__(self)
        self._data_source = data_source
        self._cols = cols
        self._type = type
        self._lookup_key = lookup_key

    def expand(self, pcoll):
        return (
            pcoll
            | 'Read {} CSV'.format(self._type) >> ReadFromText(self._data_source, skip_header_lines=1)
            | '{} to dictionary'.format(self._type) >> beam.ParDo(CsvToDict(), self._cols)
            | '{} as look-up'.format(self._type) >> beam.CombineGlobally(CollectionAsDecodeDict(self._lookup_key))
        )

# Transform nested joined datasets into a flat dict
class UnnestJoinedData(beam.DoFn):
    """Unnests source and joined data to produce a merged dict"""
    def process(self, element, source_name: str, join_name: str):
        group_key, grouped_data = element
        source_dicts = grouped_data[source_name]
        join_dicts = grouped_data[join_name]

        for sd in source_dicts:
            match = 0
            if len(join_dicts) > 0:
                match = 1
                sd.update(join_dicts[0])
            sd.update({'JOIN_MATCH': match})
            yield sd

# Composite transform for a left join operation on two dataset extracts
class LeftJoin(beam.PTransform):
    """Groups two dataset extracts on a common key and enriches left-hand side with fields from right-hand side"""

    def __init__(self, left_key: str, right_key: str):
        beam.PTransform.__init__(self)            
        self._left_key = left_key
        self._right_key = right_key

    def expand(self, pcoll):
        return (
            pcoll
            | 'Left join' >> beam.CoGroupByKey()
            | 'Unnest joined data' >> beam.ParDo(UnnestJoinedData(), self._left_key, self._right_key)
        )
    
# Transforms to aggregate variance values on a PCollection 
class AggregateVariance(beam.PTransform):
    """Aggregates variance totals using grouping fields"""

    def __init__(self, category: str, group_keys: list[str]):
        beam.PTransform.__init__(self)
        self._category = category
        self._group_keys = group_keys

    def expand(self, pcoll):
        types = [Names.TYPE_PKRD, self._category]
        return (
            pcoll
            | 'Filter for PKRD, {}'.format(self._category)
            >> beam.Filter(lambda row: Filters.filter_by_types(row, types=[Names.TYPE_PKRD, self._category], depot_type=self._category))                                                         
            | 'Variance by {}, {}'.format(self._category, self._group_keys[1])
            >> beam.GroupBy(*self._group_keys)
                        .aggregate_field('pkrd_quantity', sum, 'total_pkrd_quantity')
                        .aggregate_field('pkrd_value_tp', sum, 'total_pkrd_value_tp')
                        .aggregate_field('nfsi_quantity', sum, 'total_nfsi_quantity')
                        .aggregate_field('nfsi_value', sum, 'total_nfsi_value')
                        .aggregate_field('quantity_variance', sum, 'total_quantity_variance')
                        .aggregate_field('value_variance_tp', sum, 'total_value_variance_tp')                 
        )     
    
# Transform to generate variance totals
class CalculateVarianceTotals(beam.PTransform):
    """Aggregates category level totals for variance. Calculates GIT impact on PTD/Sales"""

    def __init__(self, report_type: str, group_key: str = 'depot_category'):
        beam.PTransform.__init__(self)
        self._report_type = report_type
        self._group_key = group_key

    def expand(self, pcoll):
        return (
            pcoll
            | 'Grand totals for {}'.format(self._report_type)
            >> beam.GroupBy(self._group_key)
                        .aggregate_field('total_pkrd_quantity', sum, 'sum_pkrd_quantity')  
                        .aggregate_field('total_pkrd_value_tp', sum, 'sum_pkrd_value_tp')
                        .aggregate_field('total_nfsi_quantity', sum, 'sum_nfsi_quantity')
                        .aggregate_field('total_nfsi_value', sum, 'sum_nfsi_value')
                        .aggregate_field('total_quantity_variance', sum, 'sum_quantity_variance')
                        .aggregate_field('total_value_variance_tp', sum, 'sum_value_variance_tp') 
                        .aggregate_field('git_quantity', sum, 'sum_git_quantity')
                        .aggregate_field('git_value', sum, 'sum_git_value') 
        ) 

# Transform PCollection to output as CSV file
def write_data_as_csv(pcoll, output: str, prefix: str):
    """Writes PCollection to CSV in target destination"""
    df = to_dataframe(pcoll, label=f'{prefix} to dataframe')
    df.to_csv(path=output,
              index=False,
              transform_label=prefix,
              num_shards=1,
              file_naming=fileio.default_file_naming(prefix=prefix, suffix=f'-{CsvFileUtils.ts_suffix()}')
    )

# Composite transform to write data to BigQuery using load job method (i.e. copies data to GCS then loads into BQ)
class LoadIntoBigQuery(beam.PTransform):
    """Composite transform to write data to BigQuery"""

    def __init__(self, table: str, dataset: str, project: str):
        beam.PTransform.__init__(self)
        self._table = table
        self._dataset = dataset
        self._project = project

    def expand(self, pcoll):
        return (
            pcoll
            | 'Write to {}.{}'.format(self._dataset, self._table)
            >> WriteToBigQuery(table=self._table,
                               dataset=self._dataset,
                               project=self._project,
                               method=WriteToBigQuery.Method.FILE_LOADS,                                                                 
                               create_disposition=BigQueryDisposition.CREATE_NEVER,
                               write_disposition=BigQueryDisposition.WRITE_APPEND
            )
        )

# fmt: on