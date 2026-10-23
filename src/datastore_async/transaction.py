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

"""Create / interact with Google Cloud Datastore transactions."""

from google.cloud.datastore.helpers import set_database_id_to_request
from google.cloud.datastore.transaction import Transaction, _make_retry_timeout_kwargs

from .batch import AsyncBatch


class AsyncTransaction(AsyncBatch, Transaction):
    async def begin_async(self, retry=None, timeout=None):
        """Begins a transaction.

        This method is called automatically when entering a with
        statement, however it can be called explicitly if you don't want
        to use a context manager. If used outside a context manager,
        `client.get` calls targeting the transaction and commit/rollback calls
        will need to be managed explicitly as well.

        :type retry: :class:`google.api_core.retry.Retry`
        :param retry:
            A retry object used to retry requests. If ``None`` is specified,
            requests will be retried using a default configuration.

        :type timeout: float
        :param timeout:
            Time, in seconds, to wait for the request to complete.
            Note that if ``retry`` is specified, the timeout applies
            to each individual attempt.

        :raises: :class:`~exceptions.ValueError` if the transaction has
                 already begun.
        """
        await super(AsyncTransaction, self).begin_async()

        kwargs = _make_retry_timeout_kwargs(retry, timeout)

        request = {
            "project_id": self.project,
            "transaction_options": self._options,
        }
        set_database_id_to_request(request, self._client.database)

        try:
            response_pb = await self._client._datastore_api_async.begin_transaction(
                request=request, **kwargs
            )
            self._id = response_pb.transaction
        except:  # noqa: E722 do not use bare except, specify exception instead
            self._status = self._ABORTED
            raise

    async def rollback_async(self, retry=None, timeout=None):
        """Rolls back the current transaction.

        This method has necessary side-effects:

        - Sets the current transaction's ID to None.

        :type retry: :class:`google.api_core.retry.Retry`
        :param retry:
            A retry object used to retry requests. If ``None`` is specified,
            requests will be retried using a default configuration.

        :type timeout: float
        :param timeout:
            Time, in seconds, to wait for the request to complete.
            Note that if ``retry`` is specified, the timeout applies
            to each individual attempt.
        """
        # if transaction has not started, abort it
        if self._status == self._INITIAL:
            self._status = self._ABORTED
            self._id = None
            return None

        kwargs = _make_retry_timeout_kwargs(retry, timeout)

        try:
            # No need to use the response it contains nothing.
            request = {
                "project_id": self.project,
                "transaction": self._id,
            }

            set_database_id_to_request(request, self._client.database)
            await self._client._datastore_api_async.rollback(request=request, **kwargs)
        finally:
            await super(AsyncTransaction, self).rollback_async()
            # Clear our own ID in case this gets accidentally reused.
            self._id = None

    async def commit_async(self, retry=None, timeout=None):
        """Commits the transaction.

        This is called automatically upon exiting a with statement,
        however it can be called explicitly if you don't want to use a
        context manager.

        This method has necessary side-effects:

        - Sets the current transaction's ID to None.

        :type retry: :class:`google.api_core.retry.Retry`
        :param retry:
            A retry object used to retry requests. If ``None`` is specified,
            requests will be retried using a default configuration.

        :type timeout: float
        :param timeout:
            Time, in seconds, to wait for the request to complete.
            Note that if ``retry`` is specified, the timeout applies
            to each individual attempt.
        """
        # if transaction has not begun, either begin now, or abort if empty
        if self._status == self._INITIAL:
            if not self._mutations:
                self._status = self._ABORTED
                self._id = None
                return None
            else:
                await self.begin_async()

        kwargs = _make_retry_timeout_kwargs(retry, timeout)

        try:
            await super(AsyncTransaction, self).commit_async(**kwargs)
        finally:
            # Clear our own ID in case this gets accidentally reused.
            self._id = None

    async def __aenter__(self):
        if not self._begin_later:
            await self.begin_async()
        self._client._push_batch(self)
        return self
