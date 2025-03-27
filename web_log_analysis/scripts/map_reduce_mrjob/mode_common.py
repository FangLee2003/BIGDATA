from mrjob.job import MRJob
import fastavro
import os
from collections import Counter

# Định nghĩa đường dẫn file
ROOT_DIR = "D:/BIGDATA/web_log_analysis"
INPUT_FILE = os.path.join(ROOT_DIR, "data/access.avro")
OUTPUT_FILE = os.path.join(ROOT_DIR, "output/mode_status.avro")
SCHEMA_FILE = os.path.join(ROOT_DIR, "schemas/mode_status_schema.avsc")


class ModeStatus(MRJob):
    def mapper(self, _, line):
        """ Đọc dữ liệu từ file Avro và trích xuất mã trạng thái (status code) """
        with open(INPUT_FILE, "rb") as f:
            reader = fastavro.reader(f)
            for record in reader:
                yield "status_code", record["status"]  # Gom nhóm để tính mode

    def reducer(self, key, statuses):
        if statuses:
            """ Tìm mode của mã trạng thái """
            counter = Counter(statuses)
            most_common_status, count = counter.most_common(1)[0]  # Mode và số lần xuất hiện
            yield most_common_status, count

    def reducer_final(self):
        """ Lưu kết quả vào file Avro """
        schema = fastavro.schema.load_schema(SCHEMA_FILE)

        # Lấy kết quả từ reducer
        records = []
        for status, count in self.reducer(None, None):
            records.append({"status": status, "count": count})

        # Ghi dữ liệu vào Avro
        with open(OUTPUT_FILE, "wb") as f:
            fastavro.writer(f, schema, records)


if __name__ == "__main__":
    ModeStatus.run()
