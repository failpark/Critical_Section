FROM python:3.12-slim

WORKDIR /app

RUN pip install uv rich

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

COPY critical/ ./critical/

ENV PYTHONUNBUFFERED=1
ENV COORD_IP=127.0.0.1
ENV COORD_PORT=50000

EXPOSE 50000/udp 50001/udp

ENTRYPOINT ["uv", "run"]
CMD ["critical/node.py"]
