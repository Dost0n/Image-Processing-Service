from prometheus_client import Counter, Gauge, Histogram


tasks_total = Counter(
    "image_tasks_total",
    "Jami qayta ishlangan ishlar",
    ["status"],
)


task_duration = Histogram(
    "image_task_duration_seconds",
    "Bitta rasmni qayta ishlash vaqti (soniya)",
)


tasks_in_progress = Gauge(
    "image_tasks_in_progress",
    "Hozir qayta ishlanayotgan ishlar",
)