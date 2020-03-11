import pandas as pd
import pytest
import sqlalchemy.exc

from ert_shared.storage import Observation
from ert_shared.storage.repository import ErtRepository

from tests.storage import db_session, engine, tables


def test_add_observation(db_session):
    repository = ErtRepository(session=db_session)
    observation = repository.add_observation(
        name="test",
        key_indexes=[0, 3],
        data_indexes=[0, 3],
        values=[22.1, 44.2],
        stds=[1, 3],
    )
    assert observation is not None


def test_add_duplicate_observation(db_session):
    repository = ErtRepository(session=db_session)
    observation = repository.add_observation(
        name="test",
        key_indexes=[0, 3],
        data_indexes=[0, 3],
        values=[22.1, 44.2],
        stds=[1, 3],
    )
    with pytest.raises(sqlalchemy.exc.IntegrityError) as error:
        observation = repository.add_observation(
            name="test",
            key_indexes=[0, 3],
            data_indexes=[0, 3],
            values=[22.1, 44.2],
            stds=[1, 3],
        )


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
    repository = ErtRepository(session=db_session)
    ensemble = repository.add_ensemble(name="test")
    realization = repository.add_realization(0, "test")
    response = repository.add_response(
        name="test",
        values=[22.1, 44.2],
        indexes=[0, 1],
        realization_index=0,
        ensemble_name="test",
    )
    assert ensemble.id is not None
    assert realization.id is not None
    assert realization.ensemble_id is not None
    assert response.id is not None
    assert response.realization_id is not None


def test_add_ensemble(db_session):
    repository = ErtRepository(session=db_session)
    ensemble = repository.add_ensemble(name="test_ensemble")
    assert ensemble.id is not None

    with pytest.raises(sqlalchemy.exc.IntegrityError) as error:
        repository.add_ensemble(name="test_ensemble")


def test_add_realization(db_session):
    with ErtRepository(db_session) as db:
        ensemble = db.add_ensemble(name="test_ensemble")
        db.commit()

        realizations = db.add_realizations([i for i in range(5)], "test_ensemble")

        db.commit()

        assert ensemble.id is not None
        for realization in realizations:
            assert realization.id is not None

    with pytest.raises(sqlalchemy.exc.IntegrityError) as error, ErtRepository(
        session=db_session
    ) as db:
        db.add_realizations([0], ensemble_name="test_ensemble")
        db.commit()


def test_add_parameter(db_session):
    repository = ErtRepository(session=db_session)
    ensemble = repository.add_ensemble(name="test")
    realization = repository.add_realization(0, "test")

    parameter = repository.add_parameter(
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
