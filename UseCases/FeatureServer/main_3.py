import json
import logging
from pathlib import Path

from pandas import DataFrame
from pyarrow import json

from API.Enums import AuthType, Environment
from API.FSClient import FSClient
from utils.decorators import print_timing


@print_timing
def download_file_from_fs(file_path: Path) -> None:
    settings_path = Path('C:\\Users\\vlaminda\\Documents\\resources\\settings_SyncOTLDataToLegacy.json')
    settings_path = Path('/home/davidlinux/Documents/AWV/resources/settings_SyncOTLDataToLegacy.json')
    fs_client = FSClient(settings_path=settings_path, auth_type=AuthType.JWT, env=Environment.PRD)
    fs_client.download_layer(layer='fietspaden_wrapp', file_path=file_path)


@print_timing
def from_file_to_df_using_pyarrow(file_path) -> DataFrame:
    import pyarrow as pa

    table = json.read_json(file_path)

    # Unnest the 'properties' column using pyarrow
    if 'properties' in table.column_names:
        # Flatten the struct column into individual columns efficiently
        properties_struct_type = table.schema.field('properties').type
        properties_chunked_array = table.column('properties')
        # Precompute all fields for all chunks to minimize Python overhead
        field_arrays = {field.name: [] for field in properties_struct_type}
        for chunk in properties_chunked_array.chunks:
            for field in properties_struct_type:
                field_arrays[field.name].append(chunk.field(field.name))
        for field in properties_struct_type:
            full_array = pa.concat_arrays(field_arrays[field.name])
            table = table.append_column(field.name, full_array)
        # Remove the original 'properties' column
        table = table.remove_column(table.schema.get_field_index('properties'))

    return table.to_pandas()


@print_timing
def main():
    logging.basicConfig(level=logging.INFO)
    file_path = Path('fietspaden_wrapp.json')

    download_file_from_fs(file_path)

    return from_file_to_df_using_pyarrow(file_path)


if __name__ == '__main__':
    df = main()
    print(df.info(verbose=True))

