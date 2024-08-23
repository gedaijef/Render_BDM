FROM python:3.9-slim

WORKDIR /bdm

COPY . .

RUN pip install -r requirements.txt

CMD ["pyhton", "ler_categorizar_enviar.py"]