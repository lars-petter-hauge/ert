from ert_shared.storage import (
    Observation,
    Realization,
    Ensemble,
    Session,
    Base,
    Response,
    Parameter,
)
from sqlalchemy import create_engine


class ErtDataSource:
    def __init__(self, session=None):

        if session is None:
            # engine = create_engine("sqlite:///:memory:", echo=True)
            engine = create_engine("sqlite:///test.db", echo=False)
            Base.metadata.create_all(engine)
            self._connection = engine.connect()
            self._session = Session(bind=self._connection)
        else:
            self._connection = None
            self._session = session

    def get_ensemble(self, name):
        return self._session.query(Ensemble).filter_by(name=name).first()

    def get_realization(self, index, ensemble_name):
        ensemble = self.get_ensemble(name=ensemble_name)
        return (
            self._session.query(Realization)
            .filter_by(ensemble_id=ensemble.id, index=index)
            .first()
        )

    def get_response(self, name, realization_index, ensemble_name):
        realization = self.get_realization(
            index=realization_index, ensemble_name=ensemble_name
        )
        return (
            self._session.query(Response)
            .filter_by(name=name, realization_id=realization.id)
            .first()
        )

    def get_parameter(self, name, group, realization_index, ensemble_name):
        realization = self.get_realization(
            index=realization_index, ensemble_name=ensemble_name
        )
        return (
            self._session.query(Response)
            .filter_by(
                name=name,
                group=group,
                realization_id=realization.id,
            )
            .first()
        )

    def get_observation(self, name):
        return self._session.query(Observation).filter_by(name=name).first()

    def add_ensemble(self, name):
        ensemble = Ensemble(name=name)
        self._session.add(ensemble)
        self._session.commit()
        return ensemble

    def add_realization(self, index, ensemble_name):
        # TODO: Look into subtransactions
        ensemble = self.get_ensemble(name=ensemble_name)

        realization = Realization(index=index, ensemble_id=ensemble.id)
        self._session.add(realization)
        self._session.commit()

        return realization

    def add_response(self, name, values, indexes, realization_index, ensemble_name, observation_id=None):
        realization = self.get_realization(
            index=realization_index, ensemble_name=ensemble_name
        )

        response = Response(
            name=name, values=values, indexes=indexes, realization_id=realization.id, observation_id=observation_id
        )
        self._session.add(response)
        self._session.commit()

        return response

    def add_parameter(self, name, group, value, realization_index, ensemble_name):
        realization = self.get_realization(
            index=realization_index, ensemble_name=ensemble_name
        )

        parameter = Parameter(
            name=name, group=group, value=value, realization_id=realization.id
        )
        self._session.add(parameter)
        self._session.commit()

        return parameter

    def add_observation(self, name, key_indexes, data_indexes, values, stds):
        observation = Observation(
            name=name,
            key_indexes=key_indexes,
            data_indexes=data_indexes,
            values=values,
            stds=stds,
        )
        self._session.add(observation)

        self._session.commit()

        return observation

    def get_all_observation_keys(self):
        return [obs.name for obs in self._session.query(Observation.name).all()]

    def __del__(self):
        if self._connection is not None:
            self._connection.close()
        if self._session is not None:
            self._session.close()

