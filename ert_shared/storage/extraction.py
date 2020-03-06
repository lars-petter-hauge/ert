from ert_shared import ERT

from ert_shared.storage.api import StorageApi
from ert_data.measured import MeasuredData


def _extract_and_dump_observations(api):
    facade = ERT.enkf_facade

    observation_keys = [
        facade.get_observation_key(nr) for nr, _ in enumerate(facade.get_observations())
    ]

    measured_data = MeasuredData(facade, observation_keys)
    measured_data.remove_inactive_observations()  # TODO: Should save all info and info about deactivation
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

    for key, parameter in parameters.items():
        group, name = key.split(":")
        for realization_index, value in parameter.iterrows():
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


def _extract_and_dump_responses(api):
    facade = ERT.enkf_facade

    ensemble_name = facade.get_current_case_name()

    gen_data_keys = [
        key for key in facade.all_data_type_keys() if facade.is_gen_data_key(key)
    ]
    # summary_data_keys = [
    #     key for key in facade.all_data_type_keys() if facade.is_summary_key(key)
    # ]

    gen_data_data = {
        key.split("@")[0]: facade.gather_gen_data_data(case=ensemble_name, key=key)
        for key in gen_data_keys
    }
    # summary_data = {
    #     key: facade.gather_summary_data(case=ensemble_name, key=key)
    #     for key in summary_data_keys
    # }

    print(gen_data_data)
    # print(summary_data)
    observation_keys = api.get_all_observation_keys()
    print(observation_keys)
    key_mapping = {facade.get_data_key_for_obs_key(key): key for key in observation_keys}

    _dump_response(api=api, responses=gen_data_data, ensemble_name=ensemble_name, key_mapping=key_mapping)


def _dump_response(api, responses, ensemble_name, key_mapping):

    ensemble = api.get_ensemble(name=ensemble_name)
    if ensemble is None:
        ensemble = api.add_ensemble(name=ensemble_name)

    for key, response in responses.items():
        for realization_index, values in response.iteritems():
            realization = api.get_realization(
                index=realization_index, ensemble_name=ensemble.name
            )
            if realization is None:
                realization = api.add_realization(
                    index=realization_index, ensemble_name=ensemble_name
                )
            observation_id = None
            if key in key_mapping:
                observation_id = api.get_observation(name=key_mapping[key]).id
            api.add_response(
                name=key,
                values=values.to_list(),
                realization_index=realization.index,
                ensemble_name=ensemble.name,
                observation_id=observation_id
            )



def dump_to_new_storage(api=None):
    if api is None:
        api = StorageApi()

    _extract_and_dump_observations(api=api)
    _extract_and_dump_parameters(api=api)
    _extract_and_dump_responses(api=api)

