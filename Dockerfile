FROM python:3.9-slim

WORKDIR /bdm

COPY . .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

CMD ["python", "ler_categorizar_enviar.py"]
