from dagster import StaticPartitionsDefinition

from climatopic.core.utilities import generate_baselines

_start_years = {
    'precipitation': 1891,
    'temperature': 1961,
}


frequency_partition_keys = ['month', 'season', 'year']

frequency_partitions_def = StaticPartitionsDefinition(frequency_partition_keys)

precipitation_baseline_partition_keys = generate_baselines(
    _start_years['precipitation']
)
temperature_baseline_partition_keys = generate_baselines(
    _start_years['temperature']
)

precipitation_baseline_partitions_def = StaticPartitionsDefinition(
    precipitation_baseline_partition_keys
)
temperature_baseline_partitions_def = StaticPartitionsDefinition(
    temperature_baseline_partition_keys
)
