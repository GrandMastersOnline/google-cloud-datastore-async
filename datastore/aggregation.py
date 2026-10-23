from google.api_core import page_iterator
from google.cloud.datastore import helpers
from google.cloud.datastore.aggregation import (
    AggregationQuery,
    AggregationResultIterator,
)
from google.cloud.datastore.query import _NOT_FINISHED
from google.cloud.datastore.query_profile import ExplainMetrics
from google.cloud.datastore_v1.types import entity as entity_pb2
from google.cloud.datastore_v1.types import query as query_pb2

from .pages_iterator import AsyncIterator


class AsyncAggregationQuery(AggregationQuery):
    def fetch(
        self,
        client=None,
        limit=None,
        eventual=False,
        retry=None,
        timeout=None,
        read_time=None,
    ):
        """Execute the Aggregation Query; return an iterator for the aggregation results.

        For example:

        .. testsetup:: aggregation-query-fetch

            import uuid

            from google.cloud import datastore

            unique = str(uuid.uuid4())[0:8]
            client = datastore.Client(namespace='ns{}'.format(unique))


        .. doctest:: aggregation-query-fetch

            >>> andy = datastore.Entity(client.key('Person', 1234))
            >>> andy['name'] = 'Andy'
            >>> sally = datastore.Entity(client.key('Person', 2345))
            >>> sally['name'] = 'Sally'
            >>> bobby = datastore.Entity(client.key('Person', 3456))
            >>> bobby['name'] = 'Bobby'
            >>> client.put_multi([andy, sally, bobby])
            >>> query = client.query(kind='Andy')
            >>> aggregation_query = client.aggregation_query(query)
            >>> result = aggregation_query.count(alias="total").fetch(limit=5)
            >>> result
            <google.cloud.datastore.aggregation.AggregationResultIterator object at ...>

        .. testcleanup:: aggregation-query-fetch

            client.delete(andy.key)

        :type client: :class:`google.cloud.datastore.client.Client`
        :param client: (Optional) client used to connect to datastore.
                       If not supplied, uses the query's value.

        :type eventual: bool
        :param eventual: (Optional) Defaults to strongly consistent (False).
                                    Setting True will use eventual consistency,
                                    but cannot be used inside a transaction or
                                    with read_time, otherwise will raise
                                    ValueError.

        :type retry: :class:`google.api_core.retry.Retry`
        :param retry:
            A retry object used to retry requests. If ``None`` is specified,
            requests will be retried using a default configuration.

        :type timeout: float
        :param timeout:
            Time, in seconds, to wait for the request to complete.
            Note that if ``retry`` is specified, the timeout applies
            to each individual attempt.

        :type read_time: datetime
        :param read_time:
            (Optional) use read_time read consistency, cannot be used inside a
            transaction or with eventual consistency, or will raise ValueError.

        :rtype: :class:`AggregationIterator`
        :returns: The iterator for the aggregation query.
        """
        if client is None:
            client = self._client

        return AsyncAggregationResultIterator(
            self,
            client,
            limit=limit,
            eventual=eventual,
            retry=retry,
            timeout=timeout,
            read_time=read_time,
        )


class AsyncAggregationResultIterator(AsyncIterator, AggregationResultIterator):
    async def _next_page_async(self):
        """Get the next page in the iterator.

        :rtype: :class:`~google.cloud.iterator.Page`
        :returns: The next page in the iterator (or :data:`None` if
                  there are no pages left).
        """
        if not self._more_results:
            return None

        transaction_id, new_transaction_options = helpers.get_transaction_options(
            self.client.current_transaction
        )
        read_options = helpers.get_read_options(
            self._eventual, transaction_id, self._read_time, new_transaction_options
        )

        partition_id = entity_pb2.PartitionId(
            project_id=self._aggregation_query.project,
            database_id=self.client.database,
            namespace_id=self._aggregation_query.namespace,
        )

        kwargs = {}

        if self._retry is not None:
            kwargs["retry"] = self._retry

        if self._timeout is not None:
            kwargs["timeout"] = self._timeout
        request = {
            "project_id": self._aggregation_query.project,
            "partition_id": partition_id,
            "read_options": read_options,
            "aggregation_query": self._build_protobuf(),
        }
        if self._aggregation_query._explain_options:
            request["explain_options"] = (
                self._aggregation_query._explain_options._to_dict()
            )
        helpers.set_database_id_to_request(request, self.client.database)

        response_pb = None

        while response_pb is None or response_pb.batch.more_results == _NOT_FINISHED:
            if response_pb is not None:
                # We haven't finished processing. A likely reason is we haven't
                # skipped all of the results yet. Don't return any results.
                # Instead, rerun query, adjusting offsets. Datastore doesn't process
                # more than 1000 skipped results in a query.
                new_query_pb = query_pb2.AggregationQuery()
                new_query_pb._pb.CopyFrom(
                    request["aggregation_query"]._pb
                )  # copy for testability
                request["aggregation_query"] = new_query_pb

            response_pb = await self.client._datastore_api_async.run_aggregation_query(
                request=request.copy(), **kwargs
            )
            # capture explain metrics if present in response
            # should only be present in last response, and only if explain_options was set
            if response_pb.explain_metrics:
                self._explain_metrics = ExplainMetrics._from_pb(
                    response_pb.explain_metrics
                )

        item_pbs = self._process_query_results(response_pb)
        return page_iterator.Page(self, item_pbs, self.item_to_value)
