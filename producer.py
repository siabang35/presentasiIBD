import os
from kafka import KafkaProducer
import json

# Inisialisasi Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Untuk metadata
)

# Path ke folder utama
train_dir = '/path/to/train'

# Iterasi melalui setiap genre
for genre in os.listdir(train_dir):
    genre_path = os.path.join(train_dir, genre)
    if os.path.isdir(genre_path):  # Pastikan ini adalah folder
        for image_file in os.listdir(genre_path):
            if image_file.endswith(('.png', '.jpg', '.jpeg')):
                image_path = os.path.join(genre_path, image_file)

                # Baca file gambar sebagai biner
                with open(image_path, 'rb') as img_file:
                    image_data = img_file.read()

                # Buat pesan metadata
                metadata = {
                    "genre": genre,
                    "file_name": image_file,
                    "file_size": len(image_data),
                }

                # Kirim metadata dan data gambar ke Kafka
                producer.send(
                    topic='art-genre-stream',
                    value={"metadata": metadata, "image_data": image_data.decode('latin-1')}
                )
                print(f"Sent image: {image_file} from genre: {genre}")

producer.close()
