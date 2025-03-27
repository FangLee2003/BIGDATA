import fastavro
import re

schema_path = "schemas/log_schema.avsc"
log_file = "data/access2.log"
avro_file = "data/access2.avro"

# Đọc schema từ file
schema = fastavro.schema.load_schema(schema_path)  # ✅ Sửa lỗi

# Regex để trích xuất thông tin từ log
log_pattern = re.compile(r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "(.*?) (.*?)" (\d+) (\d+)')

# Chuyển đổi log sang Avro
records = []
with open(log_file, "r") as f:
    for line in f:
        match = log_pattern.match(line)
        if match:
            records.append({
                "ip": match.group(1),
                "timestamp": match.group(2),
                "method": match.group(3),
                "url": match.group(4),
                "status": int(match.group(5)),
                "size": int(match.group(6))
            })

# Ghi dữ liệu vào file Avro
with open(avro_file, "wb") as out_file:
    fastavro.writer(out_file, schema, records)

print(f"Log đã được chuyển thành Avro: {avro_file}")
