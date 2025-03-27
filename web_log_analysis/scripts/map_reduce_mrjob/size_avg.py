from mrjob.job import MRJob
import fastavro
import os

# Định nghĩa đường dẫn file Avro
ROOT_DIR = "D:/BIGDATA/web_log_analysis"
INPUT_FILE = os.path.join(ROOT_DIR, "data/access.avro")
OUTPUT_FILE = os.path.join(ROOT_DIR, "output/avg_request_size.avro")
SCHEMA_FILE = os.path.join(ROOT_DIR, "schemas/avg_size_schema.avsc")

class AvgRequestSize(MRJob):
    def mapper(self, _, line):
        # Đọc dữ liệu từ file Avro
        with open(INPUT_FILE, "rb") as f:
            reader = fastavro.reader(f)
            for record in reader:
                ip = record.get("ip")
                size = record.get("size", 0)
                yield ip, (size, 1)  # (Tổng size, số lượng request)

    def reducer(self, ip, values):
        if values:
            total_size, count = 0, 0
            for size, num in values:
                total_size += size
                count += num
            yield ip, total_size / count if count > 0 else 0  # Tính trung bình

    def reducer_final(self):
        """Lưu kết quả vào file Avro"""
        # Load Avro schema
        schema = fastavro.schema.load_schema(SCHEMA_FILE)
        
        # Lưu kết quả từ reducer
        records = []
        for ip, avg_size in self.reducer(None, None):
            records.append({"ip": ip, "avg_size": avg_size})
        
        # Ghi vào file Avro
        with open(OUTPUT_FILE, "wb") as f:
            fastavro.writer(f, schema, records)

if __name__ == "__main__":
    AvgRequestSize.run()
