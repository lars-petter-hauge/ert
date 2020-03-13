import pandas as pd

from ert_shared.storage.repository import ErtRepository


class PlotStorageApi(object):

    def __init__(self, data_source=None):
        self._data_source = data_source

    def all_data_type_keys(self):
        """ Returns a list of all the keys except observation keys. For each key a dict is returned with info about
            the key"""
        # Will likely have to change this to somehow include the ensemble name
        ens = self._data_source.get_all_ensembles()[0]

        result = []

        result.extend([{
            "key": param.name,
            "index_type": None,
            "observations": [],
            "has_refcase": False,
            "dimensionality": 1,
            "metadata": {"data_origin": "Parameters"}
        } for param in ens.parameter_definitions])

        def _obs_names(obs):
            if obs is None:
                return []
            return [obs.name]

        result.extend([{
            "key": resp.name,
            "index_type": None,
            "observations": _obs_names(resp.observation),
            "has_refcase": False,
            "dimensionality": 1,
            "metadata": {"data_origin": "Response"}
        } for resp in ens.response_definitions])

        return result


    def get_all_cases_not_running(self):
        """ Returns a list of all cases that are not running. For each case a dict with info about the case is
            returned """
        return [{'has_data': True, 'hidden': False, 'name': 'default'}]


    def data_for_key(self, case, key):
        """ Returns a pandas DataFrame with the datapoints for a given key for a given case. The row index is
            the realization number, and the column index is a multi-index with (key, index/date)"""

        ens = self._data_source.get_ensemble(case)
        #test = [resp.value for real in ens.realizations for resp in real.responses if resp.name==key]
        data = [param.value for real in ens.realizations for param in real.parameters if param.name==key]

        return pd.concat({key: pd.DataFrame(data)}, axis=1).astype(float)

    def observations_for_obs_keys(self, case, obs_keys):
        """ Returns a pandas DataFrame with the datapoints for a given observation key for a given case. The row index
            is the realization number, and the column index is a multi-index with (obs_key, index/date, obs_index),
            where index/date is used to relate the observation to the data point it relates to, and obs_index is
            the index for the observation itself"""
        return pd.DataFrame()


    def _add_index_range(self, data):
        """
        Adds a second column index with which corresponds to the data
        index. This is because in libres simulated data and observations
        are connected through an observation key and data index, so having
        that information available when the data is joined is helpful.
        """
        pass


    def refcase_data(self, key):
        """ Returns a pandas DataFrame with the data points for the refcase for a given data key, if any.
            The row index is the index/date and the column index is the key."""
        return pd.DataFrame()
