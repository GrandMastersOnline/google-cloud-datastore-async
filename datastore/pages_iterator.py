import abc


class AsyncIterator(metaclass=abc.ABCMeta):
    def __aiter__(self):
        """Iterator for each item returned.

        Returns:
            types.GeneratorType[Any]: A generator of items from the API.

        Raises:
            ValueError: If the iterator has already been started.
        """
        if self._started:
            raise ValueError("Iterator has already started", self)
        self._started = True
        return self._items_iter_async()

    async def __anext__(self):
        if self.__active_iterator is None:
            self.__active_iterator = aiter(self)
        return await anext(self.__active_iterator)

    async def _items_iter_async(self):
        """Iterator for each item returned."""
        async for page in self._page_iter_async(increment=False):
            for item in page:
                self.num_results += 1
                yield item

    async def _page_iter_async(self, increment):
        """Generator of pages of API responses.

        Args:
            increment (bool): Flag indicating if the total number of results
                should be incremented on each page. This is useful since a page
                iterator will want to increment by results per page while an
                items iterator will want to increment per item.

        Yields:
            Page: each page of items from the API.
        """
        page = await self._next_page_async()
        while page is not None:
            self.page_number += 1
            if increment:
                self.num_results += page.num_items
            yield page
            page = await self._next_page_async()

    @abc.abstractmethod
    async def _next_page_async(self):
        """Get the next page in the iterator.

        This does nothing and is intended to be over-ridden by subclasses
        to return the next :class:`Page`.

        Raises:
            NotImplementedError: Always, this method is abstract.
        """
        raise NotImplementedError
