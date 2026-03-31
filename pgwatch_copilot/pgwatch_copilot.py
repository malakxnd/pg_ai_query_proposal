METRIC_INDEX = {
    "locks": ["lock", "locks", "locking", "contention"],
    "replication": ["replica", "replication", "replicate", "standby"],
}

DEFAULT_METRICS = ["cpu", "memory"]


def identify_metrics(question: str):
    question = question.lower()

    results = []

    for metric, keywords in METRIC_INDEX.items():
        if any(keyword in question for keyword in keywords):
            results.append(metric)

    return results if results else DEFAULT_METRICS