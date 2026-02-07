# Base image
FROM python:3.13.11-slim

# Set working directory
WORKDIR /app

RUN apt-get update && apt-get upgrade -y 
RUN apt-get update && apt-get install -y libcurl4-openssl-dev gcc libssl-dev
RUN apt-get install -y curl openssl
RUN apt-get install -y libgl1 libglib2.0-0
RUN apt-get update && apt-get install -y poppler-utils

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt
# RUN python -m spacy download en_core_web_sm



# ------------------------------------------------------------------
# PaddleOCR MODEL PRELOAD (CRITICAL SECTION)
# ------------------------------------------------------------------
# This guarantees zero runtime downloads

# Possible Languages
# PRELOAD_LANGS = [
#     "latin",
#     "arabic",
#     "cyrillic",
#     "devanagari",
#     "ch",
#     "ch_doc",
#     "chinese_cht",
#     "japan",
#     "korean",
#     "ta",
#     "te",
#     "ka",
# ]
RUN python - <<'EOF'
from fast_langdetect import detect
texts = [
    "This is a sample English text.",
    "هذا نص عربي نموذجي.",
    "Это пример русского текста.",
    "यह एक नमूना हिंदी पाठ है।",
    "這是一段中文範例文字。",
    "これはサンプルの日本語テキストです。",
    "이것은 샘플 한국어 텍스트입니다."
]
for text in texts:
    print(f"Detected language for '{text}': {detect(text, model="full")}")
EOF
# ------------------------------------------------------------------

# Copy CA Certificates for IBM Mysql Server
COPY 9c3ca9f5-9282-11e9-ba85-6a7166bae029 /usr/local/share/ca-certificates/ibm-mysql-ca-cert.pem

# Generate private key and CSR
RUN openssl req -new -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/O=IBM/CN=localhost"


# Generate self-signed certificate
RUN openssl x509 -req -days 365 -in server.csr -signkey server.key -out server.crt


#RUN cp server.crt /usr/local/share/ca-certificates/server.crt
#RUN cp server.key /etc/ssl/private/server.key

# Update CA Certificates
RUN update-ca-certificates
# Copy source code to working directory
COPY . .
RUN apt-get remove -y libaom3
# Expose port
EXPOSE 5005
# Start the Flask app
CMD ["python", "app.py"]
