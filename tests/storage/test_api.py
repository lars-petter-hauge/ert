import pandas as pd
import pytest
import sqlalchemy.exc

from ert_shared.storage import Observation
from ert_shared.storage.api import StorageApi

from tests.storage import db_session, engine, tables


def test_add_observation(db_session):
    api = StorageApi(session=db_session)
    observation = api.add_observation(
        name="test",
        key_indexes=[0, 3],
        data_indexes=[0, 3],
        values=[22.1, 44.2],
        stds=[1, 3],
    )
    assert observation is not None


def test_add_duplicate_observation(db_session):
    api = StorageApi(session=db_session)
    observation = api.add_observation(
        name="test",
        key_indexes=[0, 3],
        data_indexes=[0, 3],
        values=[22.1, 44.2],
        stds=[1, 3],
    )
    with pytest.raises(sqlalchemy.exc.IntegrityError) as error:
        observation = api.add_observation(
            name="test",
            key_indexes=[0, 3],
            data_indexes=[0, 3],
            values=[22.1, 44.2],
            stds=[1, 3],
        )
    assert "column name is not unique" in str(error)


observation_data = {
    ("POLY_OBS", 0, 10): {"OBS": 2.0, "STD": 0.1},
    ("POLY_OBS", 2, 12): {"OBS": 7.1, "STD": 1.1},
    ("POLY_OBS", 4, 14): {"OBS": 21.1, "STD": 4.1},
    ("POLY_OBS", 6, 16): {"OBS": 31.8, "STD": 9.1},
    ("POLY_OBS", 8, 18): {"OBS": 53.2, "STD": 16.1},
    ("TEST_OBS", 3, 3): {"OBS": 6, "STD": 0.1},
    ("TEST_OBS", 6, 6): {"OBS": 12, "STD": 0.2},
    ("TEST_OBS", 9, 9): {"OBS": 18, "STD": 0.3},
}


def test_add_response(db_session):
    api = StorageApi(session=db_session)
    ensemble = api.add_ensemble(name="test")
    realization = api.add_realization(0, "test")
    response = api.add_response(
        name="test", values=[22.1, 44.2], realization_index=0, ensemble_name="test",
    )
    assert ensemble.id is not None
    assert realization.id is not None
    assert realization.ensemble_id is not None
    assert response.id is not None
    assert response.realization_id is not None


def test_add_ensemble(db_session):
    api = StorageApi(session=db_session)
    ensemble = api.add_ensemble(name="test_ensemble")
    assert ensemble.id is not None

    with pytest.raises(sqlalchemy.exc.IntegrityError) as error:
        api.add_ensemble(name="test_ensemble")
    assert "column name is not unique" in str(error)


def test_add_realization(db_session):
    api = StorageApi(session=db_session)
    ensemble = api.add_ensemble(name="test_ensemble")
    assert ensemble.id is not None
    realization_0 = api.add_realization(0, "test_ensemble")
    realization_1 = api.add_realization(1, "test_ensemble")
    realization_2 = api.add_realization(2, "test_ensemble")
    realization_3 = api.add_realization(3, "test_ensemble")
    realization_4 = api.add_realization(4, "test_ensemble")
    assert realization_0.id is not None
    assert realization_1.id is not None
    assert realization_2.id is not None
    assert realization_3.id is not None
    assert realization_4.id is not None

    with pytest.raises(sqlalchemy.exc.IntegrityError) as error:
        api.add_realization(0, ensemble_name="test_ensemble")
    assert "columns index, ensemble_id are not unique" in str(error)


def test_add_parameter(db_session):
    api = StorageApi(session=db_session)
    ensemble = api.add_ensemble(name="test")
    realization = api.add_realization(0, "test")
    parameter = api.add_parameter(
        name="test",
        group="test_group",
        value=22.1,
        realization_index=0,
        ensemble_name="test",
    )
    assert ensemble.id is not None
    assert realization.id is not None
    assert realization.ensemble_id is not None
    assert parameter.id is not None
    assert parameter.realization_id is not None
