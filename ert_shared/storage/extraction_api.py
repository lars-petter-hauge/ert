from ert_shared import ERT

from ert_shared.storage.data_source import ErtDataSource
from ert_data.measured import MeasuredData


def _extract_and_dump_observations(data_source):
    facade = ERT.enkf_facade

    observation_keys = [
        facade.get_observation_key(nr) for nr, _ in enumerate(facade.get_observations())
    ]

    measured_data = MeasuredData(facade, observation_keys)
    measured_data.remove_inactive_observations()  # TODO: Should save all info and info about deactivation
    observations = measured_data.data.loc[["OBS", "STD"]]

    _dump_observations(data_source=data_source, observations=observations)


def _dump_observations(data_source, observations):
    for key in observations.columns.get_level_values(0).unique():
        observation = observations[key]
        if data_source.get_observation(name=key) is not None:
            continue
        data_source.add_observation(
            name=key,
            key_indexes=observation.columns.get_level_values(0).to_list(),
            data_indexes=observation.columns.get_level_values(1).to_list(),
            values=observation.loc["OBS"].to_list(),
            stds=observation.loc["STD"].to_list(),
        )


def _extract_and_dump_parameters(data_source):
    facade = ERT.enkf_facade

    ensemble_name = facade.get_current_case_name()

    parameter_keys = [
        key for key in facade.all_data_type_keys() if facade.is_gen_kw_key(key)
    ]
    all_parameters = {
        key: facade.gather_gen_kw_data(ensemble_name, key) for key in parameter_keys
    }
    print(all_parameters)

    _dump_parameters(data_source=data_source, parameters=all_parameters, ensemble_name=ensemble_name)


def _dump_parameters(data_source, parameters, ensemble_name):
    ensemble = data_source.get_ensemble(name=ensemble_name)
    if ensemble is None:
        ensemble = data_source.add_ensemble(name=ensemble_name)

    for key, parameter in parameters.items():
        group, name = key.split(":")
        for realization_index, value in parameter.iterrows():
            realization = data_source.get_realization(
                index=realization_index, ensemble_name=ensemble.name
            )
            if realization is None:
                realization = data_source.add_realization(
                    index=realization_index, ensemble_name=ensemble_name
                )
            data_source.add_parameter(
                name=name,
                group=group,
                value=value,
                realization_index=realization.index,
                ensemble_name=ensemble.name,
            )


def _extract_and_dump_responses(data_source):
    facade = ERT.enkf_facade

    ensemble_name = facade.get_current_case_name()

    gen_data_keys = [
        key for key in facade.all_data_type_keys() if facade.is_gen_data_key(key)
    ]
    summary_data_keys = [
        key for key in facade.all_data_type_keys() if facade.is_summary_key(key)
    ]

    gen_data_data = {
        key.split("@")[0]: facade.gather_gen_data_data(case=ensemble_name, key=key)
        for key in gen_data_keys
    }
    summary_data = {
        key: facade.gather_summary_data(case=ensemble_name, key=key)
        for key in summary_data_keys
    }

    # print(gen_data_data)
    observation_keys = data_source.get_all_observation_keys()
    key_mapping = {facade.get_data_key_for_obs_key(key): key for key in observation_keys}

    _dump_response(data_source=data_source, responses=gen_data_data, ensemble_name=ensemble_name, key_mapping=key_mapping)
    _dump_response(data_source=data_source, responses=summary_data, ensemble_name=ensemble_name, key_mapping=key_mapping)


def _dump_response(data_source, responses, ensemble_name, key_mapping):

    ensemble = data_source.get_ensemble(name=ensemble_name)
    if ensemble is None:
        ensemble = data_source.add_ensemble(name=ensemble_name)

    for key, response in responses.items():
        for realization_index, values in response.iteritems():
            realization = data_source.get_realization(
                index=realization_index, ensemble_name=ensemble.name
            )
            if realization is None:
                realization = data_source.add_realization(
                    index=realization_index, ensemble_name=ensemble_name
                )
            observation_id = None
            if key in key_mapping:
                observation_id = data_source.get_observation(name=key_mapping[key]).id
            data_source.add_response(
                name=key,
                values=values.to_list(),
                indexes=response.index.to_list(),
                realization_index=realization.index,
                ensemble_name=ensemble.name,
                observation_id=observation_id
            )



def dump_to_new_storage(data_source=None):
    print("Starting extraction...")
    import time
    start = time.time()
    if data_source is None:
        data_source = ErtDataSource()

    _extract_and_dump_observations(data_source=data_source)
    _extract_and_dump_parameters(data_source=data_source)
    _extract_and_dump_responses(data_source=data_source)
    end = time.time()
    print("Extraction done... {}".format(end - start))
