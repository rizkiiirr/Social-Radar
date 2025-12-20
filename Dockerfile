# Gunakan image Python yang ringan
FROM python:3.9-slim

# Set folder kerja di dalam container
WORKDIR /app

# Install dependencies sistem dasar
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy file requirements dan install library
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy seluruh file proyek ke dalam container
COPY . .

# Buat folder datalake (jika belum ada)
RUN mkdir -p datalake/bronze datalake/silver datalake/gold

# Buka port untuk Streamlit
EXPOSE 8501

# PERINTAH UTAMA:
# 1. Jalankan ELT Pipeline (Extract-Load-Transform)
# 2. Jika sukses, jalankan Streamlit App
CMD python elt_pipeline.py && streamlit run app.py --server.address=0.0.0.0