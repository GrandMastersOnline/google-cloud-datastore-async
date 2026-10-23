import contextvars

_stack = contextvars.ContextVar("stack")


class _LocalStack:
    """Manage a thread-local LIFO stack of resources.

    Intended for use in :class:`google.cloud.datastore.batch.Batch.__enter__`,
    :class:`google.cloud.storage.batch.Batch.__enter__`, etc.
    """

    def __init__(self):
        pass

    def __iter__(self):
        """Iterate the stack in LIFO order."""
        return iter(reversed(_stack.get(())))

    def push(self, resource):
        """Push a resource onto our stack."""
        try:
            stack = _stack.get()
        except LookupError:
            stack = []
            _stack.set(stack)

        stack.append(resource)

    def pop(self):
        """Pop a resource from our stack.

        :rtype: object
        :returns: the top-most resource, after removing it.
        :raises IndexError: if the stack is empty.
        """
        return _stack.get([]).pop()

    @property
    def top(self):
        """Get the top-most resource

        :rtype: object
        :returns: the top-most item, or None if the stack is empty.
        """
        stack = _stack.get(None)
        if stack:
            return stack[-1]
