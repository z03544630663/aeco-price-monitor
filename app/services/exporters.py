import csv
import io
import json

from app.config import EXPORT_FIELDS


def rows_to_csv(rows):
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=EXPORT_FIELDS)
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field) for field in EXPORT_FIELDS})
    return buffer.getvalue()


def rows_to_json(rows):
    return json.dumps(rows, ensure_ascii=False, indent=2)
