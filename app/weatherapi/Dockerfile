FROM python:3.10-slim

WORKDIR /app

RUN useradd -m -d /app appuser

COPY weatherapi/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ./shared /app/shared
COPY ./weatherapi /app/weatherapi

RUN chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

CMD ["uvicorn", "weatherapi.main:app", "--host", "0.0.0.0", "--port", "8000"]