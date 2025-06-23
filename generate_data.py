import csv
import random
import os

def generate_csv(file, rows=1000000):
    headers = [f'col_{i}' for i in range(50)]
    with open(file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for _ in range(rows):
            row = [
                random.randint(1, 100000),
                round(random.uniform(1, 9999), 2),
                f'name_{random.randint(100, 999)}',
                os.urandom(10).hex(),  # fake blob
            ] + [random.randint(0, 1000) for _ in range(46)]
            writer.writerow(row)

generate_csv('data/sample.csv')
