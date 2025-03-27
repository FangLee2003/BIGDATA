from mrjob.job import MRJob
import fastavro
import os
import sys

# Định nghĩa đường dẫn file Avro
ROOT_DIR = "D:/BIGDATA/web_log_analysis"
INPUT_FILE = os.path.join(ROOT_DIR, "data/access.avro")
OUTPUT_FILE = os.path.join(ROOT_DIR, "output/method_count.avro")
SCHEMA_FILE = os.path.join(ROOT_DIR, "schemas/method_schema.avsc")


class MethodCounter(MRJob):
    def mapper(self, _, line):
       # Đọc dữ liệu Avro từ file HDFS
        with open(INPUT_FILE, "rb") as f:
            reader = fastavro.reader(f)
            for record in reader:
                yield record["method"], 1  # (method, count)

    def reducer(self, method, counts):
        if counts:  # Kiểm tra counts có dữ liệu không
            yield method, sum(counts)


    def reducer_final(self):
        """Lưu kết quả vào file Avro"""

        # Load Avro schema từ file
        schema = fastavro.schema.load_schema(SCHEMA_FILE)

        # Lưu trữ kết quả từ reducer
        records = []
        for method, counts in self.reducer(None, None):
            total_count = sum(counts)  # Tổng số lần xuất hiện của method
            records.append({"method": method, "count": total_count})

        # Ghi dữ liệu vào file Avro
        with open(OUTPUT_FILE, "wb") as f:
            fastavro.writer(f, schema, records)

if __name__ == "__main__":
    MethodCounter.run()