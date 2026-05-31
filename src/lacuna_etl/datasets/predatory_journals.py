from lacuna_etl.datasets._predatory_base import PredatoryListPipeline
from lacuna_etl.datasets.registry import register


@register
class PredatoryJournals(PredatoryListPipeline):
    name = "predatory_journals"
    csv_name = "predatory_journals.csv"
    table_name = "predatory_journals"
    entity = "journal"
