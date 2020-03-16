import pytest
from unittest import TestCase
import pandas as pd
from ert_shared.storage.storage_api import PlotStorageApi

from tests.storage import populated_db, db_session, engine, tables


def test_all_keys(populated_db):
    api = PlotStorageApi(populated_db)
    names = set([key['key'] for key in api.all_data_type_keys()])
    assert names == set(["response_one", "response_two"])

def test_observation_values(populated_db):
    api = PlotStorageApi(populated_db)
    result = api.observations_for_obs_keys(case="ensemble_name", obs_keys=["observation_one"])
    idx = pd.MultiIndex.from_arrays([[0, 1], [0, 3]],
                                    names=['data_index', 'key_index'])
    expected = pd.DataFrame({"OBS": [10.1, 10.2], "STD": [1, 3]}, index=idx).T

    assert result.equals(expected)

def test_parameter_values():
    pass

def test_response_values(populated_db):
    api = PlotStorageApi(populated_db)
    result = api.data_for_key(case="ensemble_name", key="response_one")
    idx = pd.MultiIndex.from_arrays([["response_one", "response_one"], [0, 1]])
    expected = pd.DataFrame([11.1, 11.2], index=idx).T
    assert result.equals(expected)