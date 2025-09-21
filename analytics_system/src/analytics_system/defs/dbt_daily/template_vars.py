import dagster as dg

from analytics_system.constants import GLOBAL_END_DATE, GLOBAL_START_DATE


@dg.template_var
def daily_partitions_def() -> dg.DailyPartitionsDefinition:
    return dg.DailyPartitionsDefinition(start_date=GLOBAL_START_DATE, end_date=GLOBAL_END_DATE)