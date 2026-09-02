from lacuna_etl.datasets._predatory_base import PredatoryListPipeline
from lacuna_etl.datasets.registry import register


@register
class PredatoryPublishers(PredatoryListPipeline):
    name = "predatory_publishers"
    csv_name = "predatory_publishers.csv"
    table_name = "predatory_publishers"
    entity = "publisher"
