import sys


def reduce_counts():
	current_ip = None
	total_count = 0
	
	for line in sys.stdin:
		fields = line.strip().split("\t")
		if len(fields) != 2:
			print(f"Skipping malformed line: {line.strip()}", file=sys.stderr)
			continue  # Bỏ qua dòng lỗi
		
		ip, count = fields
		try:
			count = int(count)
		except ValueError:
			print(f"Skipping invalid count: {count}", file=sys.stderr)
			continue  # Bỏ qua nếu count không phải số
		
		if current_ip == ip:
			total_count += count
		else:
			# if current_ip:
				# print(f"{current_ip}\t{total_count}")
			current_ip = ip
			total_count = count
	
	if current_ip:
		print(f"--{current_ip}\t{total_count}")
	
	print(total_count)

if __name__ == "__main__":
	reduce_counts()
