#!/usr/bin/env python
import sys
import fastavro

def read_avro(file):
    reader = fastavro.reader(file)
    for record in reader:
        # Giả sử cần đếm số lượng request theo mỗi IP
        print(f"{record['ip']}\t1")

if __name__ == "__main__":
    read_avro(sys.stdin.buffer)

# #!/usr/bin/env python
# import sys
# import fastavro
# import time
#
# def read_avro(file_path):
#     with open(file_path, "rb") as file:
#         reader = fastavro.reader(file)
#         for record in reader:
#             print(f"{record['ip']}\t1") #, flush=True)  # Thêm flush để in ngay
#             # time.sleep(0.01)  # Tránh buffering (tùy chọn)
#
# if __name__ == "__main__":
#     read_avro(sys.argv[1])








# hdfs dfs -rm -r /user/hadoop/output



# hdfs dfs -mkdir -p /user/hadoop/input
# hdfs dfs -put data/access.avro /user/hadoop/input/


