from ert_shared import ERT

from ert_shared.storage.api import StorageApi
from ert_data.measured import MeasuredData


def _extract_and_dump_observations(api):
    facade = ERT.enkf_facade

    observation_keys = [
        facade.get_observation_key(nr) for nr, _ in enumerate(facade.get_observations())
    ]

    measured_data = MeasuredData(facade, observation_keys)
    measured_data.remove_inactive_observations()
    observations = measured_data.data.loc[["OBS", "STD"]]

    _dump_observations(api=api, observations=observations)


def _dump_observations(api, observations):
    for key in observations.columns.get_level_values(0).unique():
        observation = observations[key]
        api.add_observation(
            name=key,
            key_indexes=observation.columns.get_level_values(0).to_list(),
            data_indexes=observation.columns.get_level_values(1).to_list(),
            values=observation.loc["OBS"].to_list(),
            stds=observation.loc["STD"].to_list(),
        )


def _extract_and_dump_parameters(api):
    facade = ERT.enkf_facade

    parameter_keys = [key for key in facade.all_data_type_keys() if facade.is_gen_data_key(key)]

    measured_data = MeasuredData(facade, observation_keys)
    measured_data.remove_inactive_observations()
    parameters = measured_data.data.loc[["OBS", "STD"]]

    _dump_parameters(api=api, parameters=parameters)


def _dump_parameters(api, parameters):
    for key in parameters.columns.get_level_values(0).unique():
        observation = parameters[key]
        api.add_observation(
            name=key,
            group="", #TODO
            value=observation.loc["OBS"].to_list(),
        )


def dump_to_new_storage(api=None):
    if api is None:
        api = StorageApi()

    _extract_and_dump_observations(api=api)
