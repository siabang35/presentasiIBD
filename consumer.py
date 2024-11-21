from kafka import KafkaConsumer
import json
import os

# Inisialisasi Kafka Consumer
consumer = KafkaConsumer(
    'art-genre-stream',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='art-genre-consumer-group'
)

# Direktori output
output_dir = '/path/to/output'
os.makedirs(output_dir, exist_ok=True)

for message in consumer:
    data = message.value
    metadata = data["metadata"]
    image_data = data["image_data"].encode('latin-1')

    genre = metadata["genre"]
    file_name = metadata["file_name"]

    # Direktori berdasarkan genre
    genre_dir = os.path.join(output_dir, genre)
    os.makedirs(genre_dir, exist_ok=True)

    # Simpan file gambar
    output_path = os.path.join(genre_dir, file_name)
    with open(output_path, 'wb') as img_file:
        img_file.write(image_data)
        print(f"Saved image: {file_name} to genre: {genre}")

