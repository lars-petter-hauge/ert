import pandas as pd
import requests


class StorageClient(object):
    def __init__(self):
        self._BASE_URI = "http://api.ert/v1" # Should be localhost or something..

    def all_data_type_keys(self):
        """ Returns a list of all the keys except observation keys. For each key a dict is returned with info about
            the key

        example
        result = [
            {
                "key": "param1",
                "index_type": None,
                "observations": [],
                "has_refcase": False,
                "dimensionality": 1,
                "metadata": {"data_origin": "Parameters"}
            },
            {
                "key": "response1",
                "index_type": None,
                "observations": ["obs1"],
                "has_refcase": False,
                "dimensionality": 2,
                "metadata": {"data_origin": "Response"}
            }
        ]
        """

        r = requests.get("{base}/ensembles".format(base=self._BASE_URI))

        ens = r.json()[0]

        result = []

        r = requests.get(
            "{base}/ensembles/{ens_id}/observations".format(
                base=self._BASE_URI, ens_id=ens["id"]
            )
        )

        observations = r.json()

        # We need an overview of all the parameter/respons names. Currently these are fetched through the realization
        # entry point. We use the first realization to get them
        r = requests.get(
            "{base}/ensembles/{ens_id}/realizations".format(
                base=self._BASE_URI, ens_id=ens["id"]
            )
        )

        real = r.json()[0]

        r = requests.get(
            "{base}/ensembles/{ens_id}/realizations/{real_id}/parameters".format(
                base=self._BASE_URI, ens_id=ens["id"], real_id=real["id"]
            )
        )

        result.extend(
            [
                {
                    "key": param["name"],
                    "index_type": None,
                    "observations": [],
                    "has_refcase": False,
                    "dimensionality": 1,
                    "metadata": {"data_origin": "Parameters"},
                }
                for param in r.json()
            ]
        )

        r = requests.get(
            "{base}/ensembles/{ens_id}/realizations/{real_id}/response".format(
                base=self._BASE_URI, ens_id=ens["id"], real_id=real["id"]
            )
        )

        def _response_observations(response):
            for obs in observations:
                if response == obs["response"]:
                    return obs["value"]
            return []

        result.extend(
            [
                {
                    "key": resp["name"],
                    "index_type": None,
                    "observations": _response_observations(resp["name"]),
                    "has_refcase": False,
                    "dimensionality": 2,
                    "metadata": {"data_origin": "Reponse"},
                }
                for resp in r.json()
            ]
        )

        return result

    def get_all_cases_not_running(self):
        """ Returns a list of all cases that are not running. For each case a dict with info about the case is
        returned

        example:
        [
            {
                'has_data': True,
                'hidden': False,
                'name': <ens_name>
            },
        ]
        """

        r = requests.get("{base}/ensembles".format(self._BASE_URI))

        return [
            {"has_data": True, "hidden": False, "name": ens["name"]} for ens in r.json()
        ]

    def data_for_key(self, case, key):
        """ Returns a pandas DataFrame with the datapoints for a given key for a given case. The row index is
            the realization number, and the column index is a multi-index with (key, index/date)"""

        r = requests.get("{base}/ensembles/".format(base=self._BASE_URI))

        ens = [ens for ens in r.json() if ens["name"] == case][0]

        r = requests.get(
            "{base}/ensembles/{ens_id}/realizations".format(
                base=self._BASE_URI, ens_id=ens["id"]
            )
        )

        df = pd.DataFrame()
        for real in r.json():

            r = requests.get(
                "{base}/ensembles/{ens_id}/realizations/{real_id}".format(
                    base=self._BASE_URI, ens_id=ens["id"], real_id=real["id"]
                )
            )
            realization = r.json()

            r = requests.get(
                "{base}/ensembles/{ens_id}/realizations/{real_id}/parameters".format(
                    base=self._BASE_URI, ens_id=ens["id"], real_id=real["id"]
                )
            )
            parameters = [param for param in r.json() if param["name"] == key]

            for param in parameters:
                r = requests.get(
                    "{base}/data/{data_ref}".format(
                        base=self._BASE_URI, data_ref=param["data_ref"]
                    )
                )
                data = r.json()
                # TODO: simplified for now, expected structure not in place
                df.append(data)

            r = requests.get(
                "{base}/ensembles/{ens_id}/realizations/{real_id}/responses".format(
                    base=self._BASE_URI, ens_id=ens["id"], real_id=real["id"]
                )
            )
            responses = [resp for resp in r.json() if resp["name"] == key]

            for resp in responses:
                r = requests.get(
                    "{base}/data/{data_ref}".format(
                        base=self._BASE_URI, data_ref=resp["data_ref"]
                    )
                )
                data = r.json()
                # TODO: simplified for now, expected structure not in place
                df.append(data)

        return df

    def observations_for_obs_keys(self, case, obs_keys): #is obs_keys really plural?
        """ Returns a pandas DataFrame with the datapoints for a given observation key for a given case. The row index
            is the realization number, and the column index is a multi-index with (obs_key, index/date, obs_index),
            where index/date is used to relate the observation to the data point it relates to, and obs_index is
            the index for the observation itself"""

        r = requests.get("{base}/ensembles/".format(base=self._BASE_URI))

        ens = [ens for ens in r.json() if ens["name"] == case][0]

        r = requests.get(
            "{base}/ensembles/{ens_id}/observations".format(
                base=self._BASE_URI, ens_id=ens["id"]
            )
        )

        observations = [obs for obs in r.json() if obs["name"] in obs_keys]

        df = pd.DataFrame()
        for obs in observations:
            r = requests.get(
                "{base}/data/{data_ref}".format(
                    base=self._BASE_URI, data_ref=obs["data_ref"]
                )
            )
            # TODO: simplified for now, expected structure not in place
            df.append(r.json())

        return pd.DataFrame()

    def refcase_data(self, key):
        """ Returns a pandas DataFrame with the data points for the refcase for a given data key, if any.
            The row index is the index/date and the column index is the key."""
        return pd.DataFrame()
