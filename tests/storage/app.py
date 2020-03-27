from ert_shared.storage.storage_api import StorageApi
import flask
from flask import Response
from flask import request


def resolve_ensemble_uri(ref_pointer):
    BASE_URL = request.host_url
    return "{}ensembles/{}".format(BASE_URL, ref_pointer)

def resolve_realization_uri(base_url, ref_pointer):
    BASE_URL = base_url
    return "{}/realizations/{}".format(BASE_URL, ref_pointer)

def resolve_data_uri(struct):
    BASE_URL = request.host_url

    if isinstance(struct, list):
        for item in struct:
            resolve_data_uri(item)
    elif isinstance(struct, dict):
        for key, val in struct.items():
            if key == "data_ref":
                id = struct[key]
                url = "{}data/{}".format(BASE_URL, id)
                struct[key] = url
            else:
                resolve_data_uri(val)


class FlaskWrapper:
    def __init__(self, session):
        self.app = flask.Flask("Ert http api")
        self.app.add_url_rule('/ensembles', 'ensembles', self.ensembles)
        self.app.add_url_rule('/ensembles/<ensemble_id>', 'ensemble', self.ensemble_by_id)
        #self.app.add_url_rule('/ensembles/<ensemble_id>/realizations', 'realizations', self.realizations)
        self.app.add_url_rule('/ensembles/<ensemble_id>/realizations/<realization_idx>', 'realization', self.realization_by_id)
        self.app.add_url_rule('/data/<int:data_id>', 'data', self.data)
        self.api = StorageApi(session=session, blob_session=session)
    
    def ensembles(self):
        ensembles = self.api.ensembles()

        for index, ensemble in enumerate(ensembles):
            uri = resolve_ensemble_uri(ensemble['ref_pointer'])
            ensembles[index]['ref_pointer'] = uri
        return {"ensembles": ensembles}

    def ensemble_by_id(self, ensemble_id):
        ensemble = self.api.ensemble_schema(ensemble_id)
        base_url = resolve_ensemble_uri(ensemble_id)
        
        for index, realization in enumerate(ensemble['realizations']):
            uri = resolve_realization_uri(base_url, realization['ref_pointer'])
            ensemble['realizations'][index]['ref_pointer'] = uri
        return ensemble

    def realizations(self, ensemble_id):
        pass

    def realization_by_id(self, ensemble_id, realization_idx):
        realization = self.api.realization(ensemble_id, realization_idx, None)
        resolve_data_uri(realization)
        return realization

    def data(self, data_id):
        data = self.api.data(data_id).data
        if isinstance(data, list):
            return ",".join([str(x) for x in data])
        else:
            return str(data)
