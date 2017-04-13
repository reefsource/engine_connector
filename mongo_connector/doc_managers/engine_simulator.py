# Copyright 2013-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A class to serve as proxy for the target engine for testing.

Receives documents from the oplog worker threads and indexes them
into the backend.

Please look at the Solr and ElasticSearch doc manager classes for a sample
implementation with real systems.
"""
import subprocess
import multiprocessing
import requests
import json

from mongo_connector import constants
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase

__version__ = constants.__version__
"""DocManager Simulator version information

"""
base_url = 'http://localhost/api'
data_folder = '/home/ubuntu/data'
headers = {
    'X-SciTran-Auth' : 'coralhero2',
    'X-SciTran-Name' :'Coral Drone Reaper',
    'X-SciTran-Method':'GET'
}

def run_job(doc):
    print 'job {} started'.format(doc['_id'])
    input_file = doc['inputs'][0]
    url = '/'.join([
        base_url,
        input_file['type'] + 's',
        input_file['id'],
        'files',
        input_file['name']
    ])
    print url
    print headers
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print 'job {} failed'.format(doc['_id'])
        return
    with open('/'.join([data_folder, input_file['name']]), 'w') as f:
        f.write(r.content)
    command = ('sudo docker run --rm -v {}:/data' +
        ' -w /coralAnalysis hblasins/coral-analysis ./analyzeRAWImage' +
        ' /data/{}').format(
            data_folder,
            input_file['name']
        )
    retcode = subprocess.call(command.split())
    if retcode == 0:
        output_filename = input_file['name'].split('.')[0] + '.json'
        cont = input_file['type'] + 's'
        url = '/'.join([base_url, cont, input_file['id'], 'files'])
        print output_filename, cont, url
        files = {
            'metadata': ('', json.dumps({})),
            'file': open('/'.join([data_folder, output_filename]), 'rb')
        }
        r = requests.post(url, files=files, headers=headers)
        if r.status_code == 200:
            print 'json uploaded for job {} '.format(doc['_id'])
        output_filename = input_file['name'].split('.')[0] + '_thumb.jpg'
        print output_filename, cont, url
        files = {
            'metadata': ('', json.dumps({})),
            'file': open('/'.join([data_folder, output_filename]), 'rb')
        }
        r = requests.post(url, files=files, headers=headers)
        if r.status_code == 200:
            print 'job {} completed'.format(doc['_id'])
            return
    print 'job {} failed'.format(doc['_id'])


class DocManager(DocManagerBase):
    """BackendSimulator emulates both a target DocManager and a server.

    The DocManager class creates a connection to the backend engine and
    adds/removes documents, and in the case of rollback, searches for them.

    The reason for storing id/doc pairs as opposed to doc's is so that multiple
    updates to the same doc reflect the most up to date version as opposed to
    multiple, slightly different versions of a doc.
    """

    def __init__(self, url=None, unique_key='_id',
                 auto_commit_interval=None,
                 chunk_size=constants.DEFAULT_MAX_BULK, **kwargs):
        """Creates a dictionary to hold document id keys mapped to the
        documents as values.
        """
        self.unique_key = unique_key
        self.auto_commit_interval = auto_commit_interval
        self.url = url
        self.chunk_size = chunk_size
        self.last_doc = None
        self.kwargs = kwargs
        # Handling the parallelism directly in the connector.
        # We should change this and define a level of indirection
        # RF
        self.pool = multiprocessing.Pool(3)

    def stop(self):
        """Stops any running threads in the DocManager.
        """
        pass

    def update(self, document_id, update_spec, namespace, timestamp):
        """Apply updates given in update_spec to the document whose id
        matches that of doc.

        """
        pass

    def upsert(self, doc, namespace, timestamp):
        """Adds a document to the doc dict.
        """

        # Allow exceptions to be triggered (for testing purposes)
        if doc.get('_upsert_exception'):
            raise Exception("upsert exception")
        self.last_doc = doc
        print doc
        g = self.pool.apply_async(run_job, (doc,))

    def insert_file(self, f, namespace, timestamp):
        """Inserts a file to the doc dict.
        """
        pass

    def remove(self, document_id, namespace, timestamp):
        """Removes the document from the doc dict.
        """
        pass

    def search(self, start_ts, end_ts):
        """Searches through all documents and finds all documents that were
        modified or deleted within the range.

        Since we have very few documents in the doc dict when this is called,
        linear search is fine. This method is only used by rollbacks to query
        all the documents in the target engine within a certain timestamp
        window. The input will be two longs (converted from Bson timestamp)
        which specify the time range. The start_ts refers to the timestamp
        of the last oplog entry after a rollback. The end_ts is the timestamp
        of the last document committed to the backend.
        """
        return []

    def commit(self):
        """Simply passes since we're not using an engine that needs commiting.
        """
        pass

    def get_last_doc(self):
        """Searches through the doc dict to find the document that was
        modified or deleted most recently."""
        return self.last_doc

    def handle_command(self, command_doc, namespace, timestamp):
        pass

