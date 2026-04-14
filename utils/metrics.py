class _Counter:
    """Stub counter — replace with prometheus_client.Counter for real metrics."""

    def __init__(self, name, description, labels=()):
        self._labels = {l: self._LabeledCounter() for l in labels}

    def labels(self, **kwargs):
        key = next(iter(kwargs.values()), "")
        return self._labels.get(key, self._LabeledCounter())

    class _LabeledCounter:
        def inc(self, amount=1):
            pass


NOTIFICATION_TOTAL = _Counter(
    "notification_total",
    "Total notifications sent",
    labels=["status"],
)
