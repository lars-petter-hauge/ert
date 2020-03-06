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
        if api.get_observation(name=key) is not None:
            continue
        api.add_observation(
            name=key,
            key_indexes=observation.columns.get_level_values(0).to_list(),
            data_indexes=observation.columns.get_level_values(1).to_list(),
            values=observation.loc["OBS"].to_list(),
            stds=observation.loc["STD"].to_list(),
        )


def _extract_and_dump_parameters(api):
    facade = ERT.enkf_facade

    ensemble_name = facade.get_current_case_name()

    parameter_keys = [
        key for key in facade.all_data_type_keys() if facade.is_gen_kw_key(key)
    ]
    all_parameters = {
        key: facade.gather_gen_kw_data(ensemble_name, key) for key in parameter_keys
    }
    print(all_parameters)

    _dump_parameters(api=api, parameters=all_parameters, ensemble_name=ensemble_name)


def _dump_parameters(api, parameters, ensemble_name):
    ensemble = api.get_ensemble(name=ensemble_name)
    if ensemble is None:
        ensemble = api.add_ensemble(name=ensemble_name)

    for key, parameters in parameters.items():
        group, name = key.split(":")
        for realization_index, value in parameters.iterrows():
            realization = api.get_realization(
                index=realization_index, ensemble_name=ensemble.name
            )
            if realization is None:
                realization = api.add_realization(
                    index=realization_index, ensemble_name=ensemble_name
                )
            api.add_parameter(
                name=name,
                group=group,
                value=value,
                realization_index=realization.index,
                ensemble_name=ensemble.name,
            )


def dump_to_new_storage(api=None):
    if api is None:
        api = StorageApi()

    _extract_and_dump_observations(api=api)
    _extract_and_dump_parameters(api=api)
