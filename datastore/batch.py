# Copyright 2014 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Create / interact with a batch of updates / deletes.

Batches provide the ability to execute multiple operations
in a single request to the Cloud Datastore API.

See
https://cloud.google.com/datastore/docs/concepts/entities#batch_operations
"""

from google.cloud.datastore import Batch, helpers
from google.cloud.datastore.batch import _parse_commit_response
from google.cloud.datastore_v1.types import datastore as _datastore_pb2


class AsyncBatch(Batch):
    async def begin_async(self):
        """Begins a batch.

        This method is called automatically when entering a with
        statement, however it can be called explicitly if you don't want
        to use a context manager. If used outside a context manager,
        `client.get` calls targeting the batch and commit/rollback calls
        will need to be managed explicitly as well

        Overridden by :class:`google.cloud.datastore.transaction.Transaction`.

        :raises: :class:`ValueError` if the batch has already begun.
        """
        if self._status != self._INITIAL:
            raise ValueError("Batch already started previously.")
        self._status = self._IN_PROGRESS

    async def _commit_async(self, retry, timeout):
        """Commits the batch.

        This is called by :meth:`commit`.
        """
        if self._id is None:
            mode = _datastore_pb2.CommitRequest.Mode.NON_TRANSACTIONAL
        else:
            mode = _datastore_pb2.CommitRequest.Mode.TRANSACTIONAL

        kwargs = {}

        if retry is not None:
            kwargs["retry"] = retry

        if timeout is not None:
            kwargs["timeout"] = timeout

        request = {
            "project_id": self.project,
            "mode": mode,
            "transaction": self._id,
            "mutations": self._mutations,
        }

        helpers.set_database_id_to_request(request, self._client.database)

        commit_response_pb = await self._client._datastore_api_async.commit(
            request=request,
            **kwargs,
        )

        _, updated_keys = _parse_commit_response(commit_response_pb)
        # If the back-end returns without error, we are guaranteed that
        # ``commit`` will return keys that match (length and
        # order) directly ``_partial_key_entities``.
        for new_key_pb, entity in zip(updated_keys, self._partial_key_entities):
            new_id = new_key_pb.path[-1].id
            entity.key = entity.key.completed_key(new_id)

    async def commit_async(self, retry=None, timeout=None):
        """Commits the batch.

        This is called automatically upon exiting a with statement,
        however it can be called explicitly if you don't want to use a
        context manager.

        :type retry: :class:`google.api_core.retry.Retry`
        :param retry:
            A retry object used to retry requests. If ``None`` is specified,
            requests will be retried using a default configuration.

        :type timeout: float
        :param timeout:
            Time, in seconds, to wait for the request to complete.
            Note that if ``retry`` is specified, the timeout applies
            to each individual attempt.

        :raises: :class:`~exceptions.ValueError` if the batch is not
                 in progress.
        """
        if self._status != self._IN_PROGRESS:
            raise ValueError("Batch must be in progress to commit()")

        try:
            await self._commit_async(retry=retry, timeout=timeout)
        finally:
            self._status = self._FINISHED

    async def rollback_async(self):
        """Rolls back the current batch.

        Marks the batch as aborted (can't be used again).

        This is called automatically upon exiting a with statement,
        however it can be called explicitly if you don't want to use a
        context manager.

        Overridden by :class:`google.cloud.datastore.transaction.Transaction`.

        :raises: :class:`~exceptions.ValueError` if the batch is not
                 in progress.
        """
        if self._status != self._IN_PROGRESS:
            raise ValueError("Batch must be in progress to rollback()")

        self._status = self._ABORTED

    async def __aenter__(self):
        await self.begin_async()
        # NOTE: We make sure begin() succeeds before pushing onto the stack.
        self._client._push_batch(self)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            # commit or rollback if not in terminal state
            if self._status not in (self._ABORTED, self._FINISHED):
                if exc_type is None:
                    await self.commit_async()
                else:
                    await self.rollback_async()
        finally:
            self._client._pop_batch()
