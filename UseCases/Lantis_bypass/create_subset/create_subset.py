from otlmow_converter.OtlmowConverter import OtlmowConverter
from otlmow_model.OtlmowModel.Classes.Onderdeel.Camera import Camera
from otlmow_model.OtlmowModel.Classes.Onderdeel.Wegkantkast import Wegkantkast
from otlmow_model.OtlmowModel.Classes.Installatie.MIVInstallatie import MIVInstallatie
from otlmow_model.OtlmowModel.Classes.Legacy.Kast import Kast

from pathlib import Path

created_assets = []

camera = Camera()
camera.fill_with_dummy_data()
created_assets.append(camera)

# kast = Kast()
# kast.fill_with_dummy_data()
# created_assets.append(kast)

wegkantkast = Wegkantkast()
wegkantkast.fill_with_dummy_data()
created_assets.append(wegkantkast)

mivinstallatie = MIVInstallatie()
mivinstallatie.fill_with_dummy_data()
created_assets.append(mivinstallatie)

OtlmowConverter.from_objects_to_file(file_path=Path('Assets.xlsx'), sequence_of_objects=created_assets)